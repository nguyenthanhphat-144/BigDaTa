import os
import sys
import time
import json
import datetime

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import functions as F
from config.settings import (
    KAFKA_TOPIC_TRANSACTIONS, BRONZE_GIAO_DICH, SILVER_TRANG_THAI,
    SILVER_KET_LUAN, GOLD_DIEM_RUI_RO, GOLD_THEO_DOI_DRIFT,
    LAKEHOUSE_DIR, get_spark_session, log_pipeline_event, write_hdfs_marker
)
from core_delta.schemas import KAFKA_TX_SCHEMA, SILVER_KET_LUAN_SCHEMA, BRONZE_GIAO_DICH_SCHEMA
from core_delta.table_manager import DeltaTableManager
from data.generator import TransactionGenerator
from ml.huan_luyen import FraudModelTrainer
from ml.kiem_tra_drift import DriftDetector
from streaming.cap_nhat_trang_thai import CustomerStateUpdater
from streaming.cham_diem import RealTimeScoringEngine
from transform.bronze_to_silver import BronzeToSilverTransformer


def run_full_bootstrap():
    print("=" * 85)
    print("🚀 KHỞI ĐỘNG HỆ THỐNG PHÁT HIỆN GIAN LẬN GIAO DỊCH THẺ THỜI GIAN THỰC (bd02_gian_lan)")
    print("=" * 85)

    spark = get_spark_session("BD02_FullPipelineBootstrap")

    # 1. Initialize Delta Tables & Constraints
    print("\n[BƯỚC 1/7] Khởi tạo 5 bảng Delta Lake, Constraint CHECK (so_tien > 0) & HDFS...")
    mgr = DeltaTableManager(spark)
    mgr.initialize_all_tables()

    # 2. Train Machine Learning Models (3 Algorithms x 2 Imbalance Strategies + Point-in-Time Join)
    print("\n[BƯỚC 2/7] Xây dựng tập huấn luyện Point-in-Time Join & Huấn luyện 3x2 Mô hình Học máy...")
    trainer = FraudModelTrainer(spark)
    model_meta = trainer.train_and_evaluate_all(num_normals=12000, augment_factor=2, version_tag="model_v1")

    # 3. Replay Transactions to Kafka Topic 'transactions' (with out-of-order up to 30s)
    print("\n[BƯỚC 3/7] Phát giao dịch từ creditcard.csv vào Kafka topic 'transactions' (Turbo 300 tx/s)...")
    gen = TransactionGenerator()
    kafka_stats = gen.stream_to_kafka_turbo(target_tps=300, total_records=900, out_of_order_ratio=0.18)

    # 4. Read Kafka Batch & Update Stateful Customer Features (silver.trang_thai_khach_hang)
    print("\n[BƯỚC 4/7] Cập nhật trạng thái khách hàng (silver.trang_thai_khach_hang) qua Stateful MERGE...")
    raw_kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", KAFKA_TOPIC_TRANSACTIONS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_df = (
        raw_kafka_df.select(F.from_json(F.col("value").cast("string"), KAFKA_TX_SCHEMA).alias("tx"))
        .select("tx.*")
        .withColumn("thoi_diem", F.to_timestamp("thoi_diem"))
        .withColumn("thoi_diem_nhan", F.to_timestamp("thoi_diem_nhan"))
    )

    state_updater = CustomerStateUpdater(spark)
    state_updater.merge_batch_to_silver(parsed_df, batch_id=1)

    # 5. Score Transactions via RealTimeScoringEngine -> bronze.giao_dich + gold.diem_rui_ro + Kafka 'alerts'
    print("\n[BƯỚC 5/7] Chấm điểm rủi ro thời gian thực (cham_diem.py) -> Bronze, Gold (CDF) & Kafka 'alerts'...")
    scoring_engine = RealTimeScoringEngine(spark, model_version="model_v1")
    scoring_engine.process_micro_batch(parsed_df.limit(500), batch_id=1)

    # Seed initial reviewer feedback labels in silver.ket_luan_tham_dinh
    print("\n[BƯỚC 6/7] Khởi tạo mẫu phản hồi chuyên gia (silver.ket_luan_tham_dinh) & Kiểm tra Quarantine/Drift...")
    gold_top = (
        spark.read.format("delta").load(GOLD_DIEM_RUI_RO)
        .orderBy(F.col("diem_rui_ro").desc())
        .limit(8)
        .collect()
    )
    now_dt = datetime.datetime.now()
    sample_reviews = []
    for idx, r in enumerate(gold_top[:5]):
        is_f = 1 if idx < 4 else 0
        status_str = "GIAN_LAN" if is_f == 1 else "HOP_LE"
        sample_reviews.append((
            r["ma_giao_dich"],
            is_f,
            f"ThamDinhVien_0{(idx % 3) + 1}",
            now_dt - datetime.timedelta(minutes=15 - idx * 2),
            "Giao dịch thiết bị lạ + Z-score số tiền đột biến" if is_f == 1 else "Khách hàng xác nhận giao dịch qua tổng đài",
            random.randint(8, 25) if "random" in globals() else 14
        ))
        mgr.update_trang_thai_tham_dinh(r["ma_giao_dich"], status_str)

    if sample_reviews:
        rev_df = spark.createDataFrame(sample_reviews, SILVER_KET_LUAN_SCHEMA)
        rev_df.write.format("delta").mode("overwrite").save(SILVER_KET_LUAN)
        log_pipeline_event(
            "Web Hàng Đợi Thẩm Định (Human-in-the-Loop)",
            "Delta Lake: silver.ket_luan_tham_dinh & gold.diem_rui_ro",
            f"Chuyên gia xác nhận {len(sample_reviews)} cảnh báo (MERGE trạng thái & lưu nhãn huấn luyện)",
            "Nhãn thẩm định sẵn sàng cho chu kỳ huấn luyện lại hàng tuần trên Airflow",
            count=len(sample_reviews)
        )

    # Inject 2 duplicate + 2 invalid type records to test bronze_to_silver quarantine
    mock_hygiene = [
        ("TX_DUP_DEMO_01", now_dt, now_dt, "ACC_100001", "MERCH_200001", "THANH_TOAN", 120.0, 500.0, 380.0, 1000.0, 1120.0, "ONLINE", now_dt.date(), "DEV_1001", "192.168.1.10") + tuple(0.0 for _ in range(28)),
        ("TX_DUP_DEMO_01", now_dt, now_dt, "ACC_100001", "MERCH_200001", "THANH_TOAN", 120.0, 500.0, 380.0, 1000.0, 1120.0, "ONLINE", now_dt.date(), "DEV_1001", "192.168.1.10") + tuple(0.0 for _ in range(28)),
        ("TX_QUARANTINE_01", now_dt, now_dt, "", "MERCH_200002", "THANH_TOAN", 85.0, 500.0, 415.0, 1000.0, 1085.0, "POS", now_dt.date(), "DEV_1002", "192.168.1.11") + tuple(0.0 for _ in range(28)),
        ("TX_QUARANTINE_02", now_dt, now_dt, "ACC_100005", "MERCH_200003", "LOAI_SAI_QUY_DINH", 210.0, 500.0, 290.0, 1000.0, 1210.0, "ATM", now_dt.date(), "DEV_1003", "192.168.1.12") + tuple(0.0 for _ in range(28)),
    ]
    df_hyg = spark.createDataFrame(mock_hygiene, BRONZE_GIAO_DICH_SCHEMA)
    df_hyg.write.format("delta").partitionBy("ngay").option("mergeSchema", "true").mode("append").save(BRONZE_GIAO_DICH)

    transformer = BronzeToSilverTransformer(spark)
    hygiene_stats = transformer.process_and_quarantine()

    # 7. Run Drift Check, Constraint Test, and Z-ORDER Benchmark
    print("\n[BƯỚC 7/7] Chạy giám sát Drift (PSI/KS), Kiểm tra Delta Constraint & Tối ưu Z-ORDER...")
    drift_detector = DriftDetector(spark)
    drift_level = drift_detector.run_drift_check(inject_severe_drift=True)

    constraint_demo = mgr.demonstrate_constraint_violation(-150.0)
    zorder_bench = mgr.optimize_and_zorder(BRONZE_GIAO_DICH, ["tai_khoan_nguon", "thoi_diem"])

    # Save cached operational summary for instant Flask Web UI rendering
    summary_cache = {
        "bootstrapped_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kafka_stats": kafka_stats,
        "hygiene_stats": hygiene_stats,
        "drift_level": drift_level,
        "constraint_demo": constraint_demo,
        "zorder_benchmark": zorder_bench
    }
    with open(os.path.join(LAKEHOUSE_DIR, "operational_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary_cache, f, indent=2, ensure_ascii=False, default=str)

    write_hdfs_marker("audit/bootstrap_summary.json", summary_cache)
    print("\n✅ KHỞI TẠO TOÀN BỘ PIPELINE THÀNH CÔNG!")
    spark.stop()


if __name__ == "__main__":
    run_full_bootstrap()
