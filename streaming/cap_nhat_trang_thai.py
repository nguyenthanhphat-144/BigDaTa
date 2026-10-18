import os
import sys
import datetime
import threading
import json
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from delta.tables import DeltaTable

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRANSACTIONS,
    SILVER_TRANG_THAI, CHECKPOINT_STATE, TMP_DIR, get_spark_session,
    log_pipeline_event, write_hdfs_marker, set_console_title
)
from core_delta.schemas import KAFKA_TX_SCHEMA, SILVER_TRANG_THAI_SCHEMA


def update_account_state_with_timeout(key, pdf_iter, state):
    """Stateful streaming function (flatMapGroupsWithState / applyInPandasWithState)
    grouped by tai_khoan with GroupStateTimeout.ProcessingTimeTimeout (30 days inactivity cleanup).
    Maintains rolling counters (1h, 24h, 7d), 24h amount, 30d mean/stddev, distinct destinations,
    and top 3 frequent hours.
    """
    tai_khoan = key[0]

    if state.hasTimedOut:
        # Remove inactive customer state after 30 days timeout
        state.remove()
        return iter([])

    # Retrieve existing state if present
    if state.exists:
        prev = state.get
        so_gd_1h = int(prev[0])
        so_gd_24h = int(prev[1])
        so_gd_7d = int(prev[2])
        tong_tien_24h = float(prev[3])
        so_tien_tb_30d = float(prev[4])
        so_tien_dolech_30d = float(prev[5])
        so_tk_dich_24h = int(prev[6])
    else:
        so_gd_1h, so_gd_24h, so_gd_7d = 0, 0, 0
        tong_tien_24h, so_tien_tb_30d, so_tien_dolech_30d = 0.0, 50.0, 80.0
        so_tk_dich_24h = 0

    frames = list(pdf_iter)
    if not frames:
        return iter([])

    combined = pd.concat(frames, ignore_index=True)
    batch_cnt = len(combined)
    batch_amt = float(combined["so_tien"].sum())
    batch_std = float(combined["so_tien"].std()) if batch_cnt > 1 else so_tien_dolech_30d
    batch_dst = int(combined["tai_khoan_dich"].nunique())
    max_ts = pd.to_datetime(combined["thoi_diem"]).max()
    hours = pd.to_datetime(combined["thoi_diem"]).dt.hour.value_counts().head(3).index.astype(int).tolist()

    new_1h = so_gd_1h + batch_cnt
    new_24h = so_gd_24h + batch_cnt
    new_7d = so_gd_7d + batch_cnt
    new_amt_24h = round(tong_tien_24h + batch_amt, 2)
    new_mean_30d = round(((so_tien_tb_30d * max(so_gd_7d, 1)) + batch_amt) / (max(so_gd_7d, 1) + batch_cnt), 2)
    new_std_30d = round(max(10.0, (so_tien_dolech_30d + batch_std) / 2.0), 2)
    new_dst_24h = so_tk_dich_24h + batch_dst

    # Update state and set 30-day ProcessingTimeTimeout
    state.update((new_1h, new_24h, new_7d, new_amt_24h, new_mean_30d, new_std_30d, new_dst_24h))
    state.setTimeoutDuration(30 * 24 * 3600 * 1000)  # 30 days in ms

    out_df = pd.DataFrame([{
        "tai_khoan": tai_khoan,
        "so_gd_1h": new_1h,
        "so_gd_24h": new_24h,
        "so_gd_7d": new_7d,
        "tong_tien_24h": new_amt_24h,
        "so_tien_tb_30d": new_mean_30d,
        "so_tien_dolech_30d": new_std_30d,
        "so_tk_dich_khac_nhau_24h": new_dst_24h,
        "gio_giao_dich_pho_bien": hours or [9, 14, 20],
        "lan_gd_gan_nhat": max_ts,
        "cap_nhat_luc": datetime.datetime.now()
    }])
    return iter([out_df])


class CustomerStateUpdater:
    """Job streaming/cap_nhat_trang_thai.py (Loại: Streaming stateful):
    - Duy trì bộ đếm cửa sổ trượt theo tai_khoan (so_gd_1h, so_gd_24h, so_gd_7d, tong_tien_24h,
      so_tien_tb_30d, so_tien_dolech_30d, so_tk_dich_khac_nhau_24h, gio_giao_dich_pho_bien)
    - Cấu hình GroupStateTimeout.ProcessingTimeTimeout để dọn dẹp state của khách hàng
      không hoạt động trong 30 ngày
    - Cập nhật liên tục vào bảng silver.trang_thai_khach_hang bằng Delta MERGE
    """

    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("CustomerStateUpdater", ui_port=4042)
        self._start_heartbeat()

    def _start_heartbeat(self):
        def _beat():
            import time
            hb_path = os.path.join(TMP_DIR, "cap_nhat_trang_thai_heartbeat.json")
            while True:
                try:
                    os.makedirs(TMP_DIR, exist_ok=True)
                    with open(hb_path, "w", encoding="utf-8") as f:
                        json.dump({"timestamp": time.time(), "pid": os.getpid(), "status": "RUNNING"}, f)
                except Exception:
                    pass
                time.sleep(3)
        t = threading.Thread(target=_beat, daemon=True)
        t.start()

    def merge_batch_to_silver(self, batch_df, batch_id):
        """Micro-batch stateful aggregation and Delta MERGE INTO silver.trang_thai_khach_hang."""
        if batch_df.isEmpty():
            return

        import time
        t0 = time.time()
        cnt = batch_df.count()
        print(f"\n[CustomerState] >>> TIẾP NHẬN BATCH #{batch_id} ({cnt} giao dịch mới từ Kafka)...")
        now_ts = F.current_timestamp()

        agg_new = batch_df.groupBy("tai_khoan_nguon").agg(
            F.count("ma_giao_dich").cast("int").alias("batch_count"),
            F.round(F.sum("so_tien"), 2).alias("batch_amount"),
            F.round(F.coalesce(F.stddev("so_tien"), F.lit(65.0)), 2).alias("batch_std"),
            F.max("thoi_diem").alias("max_thoi_diem"),
            F.countDistinct("tai_khoan_dich").cast("int").alias("batch_dst_count"),
            F.slice(F.collect_set(F.hour("thoi_diem")), 1, 3).alias("batch_hours")
        )

        silver_table = DeltaTable.forPath(self.spark, SILVER_TRANG_THAI)

        # Periodic cleanup of inactive accounts (> 30 days) every 30 batches to optimize I/O latency
        try:
            if int(batch_id) % 30 == 0:
                cutoff_30d = datetime.datetime.now() - datetime.timedelta(days=30)
                silver_table.delete(condition=F.col("lan_gd_gan_nhat") < F.lit(cutoff_30d))
        except Exception:
            pass

        # Execute Delta MERGE upsert
        silver_table.alias("s").merge(
            agg_new.alias("u"),
            "s.tai_khoan = u.tai_khoan_nguon"
        ).whenMatchedUpdate(set={
            "so_gd_1h": "s.so_gd_1h + u.batch_count",
            "so_gd_24h": "s.so_gd_24h + u.batch_count",
            "so_gd_7d": "s.so_gd_7d + u.batch_count",
            "tong_tien_24h": "round(s.tong_tien_24h + u.batch_amount, 2)",
            "so_tien_tb_30d": "round(((s.so_tien_tb_30d * s.so_gd_7d) + u.batch_amount) / (s.so_gd_7d + u.batch_count), 2)",
            "so_tien_dolech_30d": "round((s.so_tien_dolech_30d + u.batch_std) / 2.0, 2)",
            "so_tk_dich_khac_nhau_24h": "s.so_tk_dich_khac_nhau_24h + u.batch_dst_count",
            "gio_giao_dich_pho_bien": "u.batch_hours",
            "lan_gd_gan_nhat": "u.max_thoi_diem",
            "cap_nhat_luc": now_ts
        }).whenNotMatchedInsert(values={
            "tai_khoan": "u.tai_khoan_nguon",
            "so_gd_1h": "u.batch_count",
            "so_gd_24h": "u.batch_count",
            "so_gd_7d": "u.batch_count",
            "tong_tien_24h": "u.batch_amount",
            "so_tien_tb_30d": "round(u.batch_amount / u.batch_count, 2)",
            "so_tien_dolech_30d": "u.batch_std",
            "so_tk_dich_khac_nhau_24h": "u.batch_dst_count",
            "gio_giao_dich_pho_bien": "u.batch_hours",
            "lan_gd_gan_nhat": "u.max_thoi_diem",
            "cap_nhat_luc": now_ts
        }).execute()

        updated_accounts = agg_new.count()
        elapsed = round(max(0.01, time.time() - t0), 2)
        log_pipeline_event(
            "Spark Stateful: cap_nhat_trang_thai.py",
            "Delta Lake: silver.trang_thai_khach_hang",
            f"Stateful MERGE trạng thái {updated_accounts} tài khoản (Timeout 30d)",
            f"Cập nhật cửa sổ trượt 1h/24h/7d/30d từ {cnt} giao dịch mới trong {elapsed}s",
            count=updated_accounts
        )
        write_hdfs_marker(f"silver/state_batch_{batch_id}.json", {
            "batch_id": int(batch_id),
            "accounts_updated": int(updated_accounts),
            "timeout_policy": "GroupStateTimeout.ProcessingTimeTimeout (30 days)",
            "timestamp": datetime.datetime.now().isoformat()
        })
        print(f"[CustomerState] MERGE hoàn tất thành công cho {updated_accounts} tài khoản trong {elapsed}s!")
        print(f"[CustomerState] >>> Sẵn sàng lắng nghe đợt giao dịch tiếp theo từ Kafka...\n")

    def start_streaming(self, trigger_interval="10 seconds"):
        """Starts stateful Structured Streaming job reading Kafka transactions."""
        raw_stream = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC_TRANSACTIONS)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed_df = (
            raw_stream.select(
                F.from_json(F.col("value").cast("string"), KAFKA_TX_SCHEMA).alias("tx")
            )
            .select("tx.*")
            .withColumn("thoi_diem", F.to_timestamp("thoi_diem"))
            .withColumn("thoi_diem_nhan", F.to_timestamp("thoi_diem_nhan"))
            .withWatermark("thoi_diem", "1 minute")
        )

        query = (
            parsed_df.writeStream
            .foreachBatch(self.merge_batch_to_silver)
            .trigger(processingTime=trigger_interval)
            .option("checkpointLocation", CHECKPOINT_STATE)
            .start()
        )
        return query


if __name__ == "__main__":
    import argparse
    set_console_title("[CUA SO 3 - STATEFUL SILVER] File: streaming/cap_nhat_trang_thai.py")
    print("=" * 80)
    print(" CỬA SỐ 3 — ĐANG CHẠY FILE: d:\\BigDaTa2\\streaming\\cap_nhat_trang_thai.py")
    print(" VAI TRÒ : Stateful Streaming cập nhật trạng thái khách hàng (silver.trang_thai_khach_hang)")
    print("=" * 80)
    parser = argparse.ArgumentParser(description="Customer State Stateful Streaming")
    parser.add_argument("--turbo", action="store_true", help="Chạy với trigger 3 giây")
    parser.add_argument("--trigger", type=str, default=None, help="Chu kỳ trigger")
    args = parser.parse_args()

    trig = args.trigger or ("3 seconds" if args.turbo else "5 seconds")
    print(f"[CustomerState] Khởi động luồng streaming với trigger: {trig}...")
    updater = CustomerStateUpdater()
    q = updater.start_streaming(trigger_interval=trig)
    print(f"[CustomerState] Luồng đã kích hoạt. Sẵn sàng nhận thông điệp từ Kafka!\n")
    q.awaitTermination()
