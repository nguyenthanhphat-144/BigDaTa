import os
import sys
import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import functions as F
from delta.tables import DeltaTable

from config.settings import (
    BRONZE_GIAO_DICH, QUARANTINE_DIR, get_spark_session,
    log_pipeline_event, write_hdfs_marker, set_console_title
)


class BronzeToSilverTransformer:
    """Job transform/bronze_to_silver.py (Loại: Batch):
    Chuẩn hóa dữ liệu, phát hiện bản ghi trùng lặp (ma_giao_dich),
    ghi các bản ghi lỗi vào _quarantine.
    """

    VALID_LOAI = ["CHUYEN_KHOAN", "RUT_TIEN", "THANH_TOAN", "NAP_TIEN"]
    VALID_KENH = ["ATM", "POS", "ONLINE", "APP"]

    def __init__(self, spark=None):
        set_console_title("[CUA SO 3 - CHUAN HOA BRONZE -> SILVER] File: transform/bronze_to_silver.py")
        self.spark = spark or get_spark_session("BronzeToSilverTransformer")
        if not str(QUARANTINE_DIR).startswith("hdfs://"):
            os.makedirs(QUARANTINE_DIR, exist_ok=True)

    def process_and_quarantine(self):
        """Reads bronze.giao_dich, isolates invalid/corrupt records into _quarantine,
        deduplicates valid records, and logs data hygiene statistics.
        """
        print("=== BẮT ĐẦU XỬ LÝ BATCH: BRONZE TO SILVER & QUARANTINE ===")
        if not DeltaTable.isDeltaTable(self.spark, BRONZE_GIAO_DICH):
            return {"total_raw": 0, "invalid_quarantined": 0, "duplicates_removed": 0, "clean_records": 0}

        raw_df = self.spark.read.format("delta").load(BRONZE_GIAO_DICH)
        total_raw = raw_df.count()
        if total_raw == 0:
            return {"total_raw": 0, "invalid_quarantined": 0, "duplicates_removed": 0, "clean_records": 0}

        cond_valid_amount = F.col("so_tien") > 0.0
        cond_valid_acc_src = F.col("tai_khoan_nguon").isNotNull() & (F.length(F.trim(F.col("tai_khoan_nguon"))) > 0)
        cond_valid_acc_dst = F.col("tai_khoan_dich").isNotNull() & (F.length(F.trim(F.col("tai_khoan_dich"))) > 0)
        cond_valid_loai = F.col("loai").isin(self.VALID_LOAI)

        flagged_df = raw_df.withColumn(
            "quarantine_reason",
            F.when(~cond_valid_amount, F.lit("SO_TIEN_KHONG_HOP_LE"))
             .when(~cond_valid_acc_src, F.lit("THIEU_TAI_KHOAN_NGUON"))
             .when(~cond_valid_acc_dst, F.lit("THIEU_TAI_KHOAN_DICH"))
             .when(~cond_valid_loai, F.lit("LOAI_GD_KHONG_XAC_DINH"))
             .otherwise(F.lit("HOP_LE"))
        )

        invalid_df = flagged_df.filter(F.col("quarantine_reason") != "HOP_LE")
        valid_candidates = flagged_df.filter(F.col("quarantine_reason") == "HOP_LE")

        invalid_count = invalid_df.count()
        if invalid_count > 0:
            invalid_df.write.format("delta").option("mergeSchema", "true").mode("append").save(QUARANTINE_DIR)

        valid_count = valid_candidates.count()
        dedup_df = valid_candidates.dropDuplicates(["ma_giao_dich"])
        dedup_count = dedup_df.count()
        duplicates_removed = valid_count - dedup_count

        stats = {
            "total_raw": int(total_raw),
            "invalid_quarantined": int(invalid_count),
            "duplicates_removed": int(duplicates_removed),
            "clean_records": int(dedup_count)
        }

        log_pipeline_event(
            "Delta Lake: bronze.giao_dich",
            "Job: transform/bronze_to_silver.py -> _quarantine",
            f"Chuẩn hóa & Khử trùng lặp ({dedup_count} sạch, {duplicates_removed} trùng, {invalid_count} cách ly)",
            f"Các bản ghi vi phạm cấu trúc được đẩy sang bảng cách ly _quarantine",
            count=total_raw
        )
        write_hdfs_marker("silver/hygiene_report.json", stats)
        return stats


if __name__ == "__main__":
    set_console_title("[CUA SO 3 - CHUAN HOA BRONZE -> SILVER] File: transform/bronze_to_silver.py")
    print("=" * 80)
    print(" CỬA SỐ 3 — ĐANG CHẠY FILE: d:\\BigDaTa2\\transform\\bronze_to_silver.py")
    print(" VAI TRÒ : Chuẩn hóa dữ liệu Bronze -> Silver & lọc bản ghi lỗi sang _quarantine")
    print("=" * 80)
    transformer = BronzeToSilverTransformer()
    print(transformer.process_and_quarantine())
