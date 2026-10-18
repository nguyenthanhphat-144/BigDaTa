import os
import sys
import random
import datetime
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from config.settings import (
    BRONZE_GIAO_DICH, SILVER_TRANG_THAI, SILVER_KET_LUAN,
    SILVER_TAP_HUAN_LUYEN, CREDITCARD_CSV, get_spark_session,
    log_pipeline_event, write_hdfs_marker
)


class TrainingDatasetBuilder:
    """Job ml/tao_tap_huan_luyen.py (Loại: Batch):
    - Ghép giao dịch và trạng thái khách hàng tại ĐÚNG thời điểm trước giao dịch xảy ra
      (Point-in-Time Join bằng Window rangeBetween[-W, -1] nhằm chống Data Leakage)
    - Kết hợp nhãn phản hồi từ thẩm định viên (silver.ket_luan_tham_dinh)
    - Lưu snapshot tập huấn luyện vào bảng Delta silver.tap_huan_luyen để phục vụ
      kiểm toán Time Travel (SELECT * FROM ... VERSION AS OF <v_luc_huan_luyen>)
    """

    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("TrainingDatasetBuilder")

    def build_point_in_time_features(self, transactions_df):
        """Computes rolling customer behavioral features strictly BEFORE each transaction's
        timestamp T: rangeBetween(-WindowSeconds, -1).
        Guarantees zero future or current transaction leakage into historical features.
        """
        print("[Point-in-Time Join] Tính đặc trưng cửa sổ trượt nghiêm ngặt tại [T - Window, T - 1s]...")
        df = transactions_df.withColumn("ts_sec", F.col("thoi_diem").cast("long"))

        w_1h = Window.partitionBy("tai_khoan_nguon").orderBy("ts_sec").rangeBetween(-3600, -1)
        w_24h = Window.partitionBy("tai_khoan_nguon").orderBy("ts_sec").rangeBetween(-86400, -1)
        w_7d = Window.partitionBy("tai_khoan_nguon").orderBy("ts_sec").rangeBetween(-7 * 86400, -1)
        w_30d = Window.partitionBy("tai_khoan_nguon").orderBy("ts_sec").rangeBetween(-30 * 86400, -1)

        pit_df = (
            df
            .withColumn("so_gd_1h", F.coalesce(F.count("ma_giao_dich").over(w_1h), F.lit(0)).cast("int"))
            .withColumn("so_gd_24h", F.coalesce(F.count("ma_giao_dich").over(w_24h), F.lit(0)).cast("int"))
            .withColumn("so_gd_7d", F.coalesce(F.count("ma_giao_dich").over(w_7d), F.lit(0)).cast("int"))
            .withColumn("tong_tien_24h", F.round(F.coalesce(F.sum("so_tien").over(w_24h), F.lit(0.0)), 2))
            .withColumn("so_tien_tb_30d", F.round(F.coalesce(F.avg("so_tien").over(w_30d), F.lit(50.0)), 2))
            .withColumn("so_tien_dolech_30d", F.round(F.coalesce(F.stddev("so_tien").over(w_30d), F.lit(25.0)), 2))
            .withColumn(
                "z_score_so_tien",
                F.round(
                    F.when(
                        F.col("so_tien_dolech_30d") > 0.001,
                        (F.col("so_tien") - F.col("so_tien_tb_30d")) / F.col("so_tien_dolech_30d")
                    ).otherwise(0.0),
                    4
                )
            )
            .drop("ts_sec")
        )
        return pit_df

    def load_labeled_dataset(self, num_normals=15000, augment_factor=2, save_delta_snapshot=True):
        """Builds point-in-time training dataset from creditcard.csv + reviewer labels
        in silver.ket_luan_tham_dinh, and saves a Delta Lake versioned snapshot for Time Travel.
        """
        print(f"[DatasetBuilder] Đọc nguồn dữ liệu gốc từ {CREDITCARD_CSV}...")
        pdf = pd.read_csv(CREDITCARD_CSV)
        frauds = pdf[pdf["Class"] == 1]
        normals = pdf[pdf["Class"] == 0]

        np.random.seed(42)
        fraud_list = [frauds]
        if augment_factor > 1:
            for _ in range(augment_factor - 1):
                f_aug = frauds.copy()
                for c in [f"V{i}" for i in range(1, 29)]:
                    f_aug[c] += np.random.normal(0, 0.02, len(f_aug))
                fraud_list.append(f_aug)
        all_frauds = pd.concat(fraud_list).sort_values("Time").reset_index(drop=True)

        idx_sample = np.linspace(0, len(normals) - 1, num_normals).astype(int)
        sampled_normals = normals.iloc[idx_sample]

        combined_pdf = pd.concat([all_frauds, sampled_normals]).sort_values("Time").reset_index(drop=True)

        base_time = datetime.datetime(2026, 10, 1, 8, 0, 0)
        combined_pdf["thoi_diem"] = combined_pdf["Time"].apply(
            lambda t: base_time + datetime.timedelta(seconds=float(t))
        )
        combined_pdf["ngay"] = combined_pdf["thoi_diem"].dt.date
        combined_pdf["so_tien"] = combined_pdf["Amount"].apply(lambda a: max(float(a), 0.01))
        combined_pdf["is_fraud"] = combined_pdf["Class"].astype(int)

        random.seed(42)
        accounts = [f"ACC_{100000 + (i % 1200)}" for i in range(len(combined_pdf))]
        merchants = [f"MERCH_{200000 + (i % 400)}" for i in range(len(combined_pdf))]
        combined_pdf["tai_khoan_nguon"] = accounts
        combined_pdf["tai_khoan_dich"] = merchants
        combined_pdf["ma_giao_dich"] = [f"TX_{i:08d}" for i in range(len(combined_pdf))]
        combined_pdf["loai"] = "THANH_TOAN"
        combined_pdf["kenh"] = "ONLINE"

        rename_dict = {f"V{i}": f"v{i}" for i in range(1, 29)}
        combined_pdf.rename(columns=rename_dict, inplace=True)

        cols_to_keep = [
            "ma_giao_dich", "thoi_diem", "tai_khoan_nguon", "tai_khoan_dich",
            "loai", "kenh", "so_tien", "ngay", "is_fraud"
        ] + [f"v{i}" for i in range(1, 29)]
        combined_pdf = combined_pdf[cols_to_keep]

        sdf = self.spark.createDataFrame(combined_pdf)
        pit_sdf = self.build_point_in_time_features(sdf)

        # Merge verified reviewer feedback labels from silver.ket_luan_tham_dinh if available
        reviewer_feedback_count = 0
        if DeltaTable.isDeltaTable(self.spark, SILVER_KET_LUAN):
            fb_df = (
                self.spark.read.format("delta").load(SILVER_KET_LUAN)
                .select(
                    F.col("ma_giao_dich").alias("fb_ma_gd"),
                    F.col("nhan").alias("fb_nhan")
                )
                .dropDuplicates(["fb_ma_gd"])
            )
            reviewer_feedback_count = fb_df.count()
            if reviewer_feedback_count > 0:
                pit_sdf = (
                    pit_sdf.join(fb_df, pit_sdf.ma_giao_dich == fb_df.fb_ma_gd, how="left")
                    .withColumn("is_fraud", F.coalesce(F.col("fb_nhan"), F.col("is_fraud")))
                    .drop("fb_ma_gd", "fb_nhan")
                )

        # Save snapshot to Delta Lake table silver.tap_huan_luyen for Time Travel audit
        delta_version = 0
        if save_delta_snapshot:
            pit_sdf.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(SILVER_TAP_HUAN_LUYEN)
            hist = DeltaTable.forPath(self.spark, SILVER_TAP_HUAN_LUYEN).history(1).collect()
            if hist:
                delta_version = int(hist[0]["version"])

            log_pipeline_event(
                "silver.ket_luan_tham_dinh + bronze.giao_dich",
                "Job: ml/tao_tap_huan_luyen.py (Point-in-Time Join)",
                f"Tạo tập huấn luyện chống Data Leakage (Delta Version v{delta_version})",
                f"Tổng {len(combined_pdf)} mẫu | Nhãn chuyên gia kết hợp: {reviewer_feedback_count}",
                count=len(combined_pdf)
            )
            write_hdfs_marker(f"silver/training_snapshot_v{delta_version}.json", {
                "delta_table": "silver.tap_huan_luyen",
                "delta_version": delta_version,
                "total_samples": len(combined_pdf),
                "reviewer_labels_merged": reviewer_feedback_count,
                "created_at": datetime.datetime.now().isoformat()
            })

        return pit_sdf, delta_version, reviewer_feedback_count

    def demonstrate_point_in_time_example(self):
        """Returns a concrete 3-transaction walkthrough proving Point-in-Time Join
        prevents temporal data leakage.
        """
        t1 = datetime.datetime(2026, 10, 16, 10, 0, 0)
        t2 = datetime.datetime(2026, 10, 16, 10, 15, 0)  # +15 mins
        t3 = datetime.datetime(2026, 10, 16, 12, 0, 0)   # +2 hours

        sample_txs = [
            ("TX_DEMO_01", t1, "ACC_DEMO_999", "MERCH_01", "THANH_TOAN", "ONLINE", 100.0, t1.date(), 0),
            ("TX_DEMO_02", t2, "ACC_DEMO_999", "MERCH_02", "CHUYEN_KHOAN", "APP", 250.0, t2.date(), 1),
            ("TX_DEMO_03", t3, "ACC_DEMO_999", "MERCH_03", "RUT_TIEN", "ATM", 400.0, t3.date(), 0),
        ]
        full_records = [r + tuple(0.0 for _ in range(28)) for r in sample_txs]
        cols = [
            "ma_giao_dich", "thoi_diem", "tai_khoan_nguon", "tai_khoan_dich",
            "loai", "kenh", "so_tien", "ngay", "is_fraud"
        ] + [f"v{i}" for i in range(1, 29)]

        df = self.spark.createDataFrame(full_records, cols)
        pit_pdf = (
            self.build_point_in_time_features(df)
            .select("ma_giao_dich", "tai_khoan_nguon", "thoi_diem", "so_tien", "so_gd_1h", "so_gd_24h", "tong_tien_24h", "is_fraud")
            .orderBy("thoi_diem")
            .toPandas()
        )
        pit_pdf["thoi_diem"] = pit_pdf["thoi_diem"].astype(str)
        return pit_pdf.to_dict(orient="records")


if __name__ == "__main__":
    builder = TrainingDatasetBuilder()
    example = builder.demonstrate_point_in_time_example()
    print("=== VÍ DỤ CHỨNG MINH POINT-IN-TIME JOIN (CHỐNG DATA LEAKAGE) ===")
    for row in example:
        print(row)
