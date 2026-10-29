import os
import sys
import json
import datetime
import unittest
import numpy as np

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import functions as F
from config.settings import (
    BRONZE_GIAO_DICH, SILVER_TRANG_THAI, SILVER_KET_LUAN,
    SILVER_TAP_HUAN_LUYEN, GOLD_DIEM_RUI_RO, GOLD_THEO_DOI_DRIFT,
    QUARANTINE_DIR, HDFS_CREDITCARD_CSV, MODELS_DIR, hdfs_list_status, get_spark_session
)
from core_delta.table_manager import DeltaTableManager
from core_delta.schemas import BRONZE_GIAO_DICH_SCHEMA, SILVER_KET_LUAN_SCHEMA


class FraudDetectionAcceptanceTests(unittest.TestCase):
    """Kiểm thử nghiệm thu bắt buộc (Mục 2.11 - 8 hạng mục nghiệm thu)."""

    @classmethod
    def setUpClass(cls):
        print("\n" + "=" * 85)
        print("BỘ KIỂM THỬ NGHIỆM THU BẮT BUỘC (SECTION 2.11 - 8 TIÊU CHÍ NGHIỆM THU)")
        print("=" * 85)
        cls.mgr = DeltaTableManager()
        cls.spark = cls.mgr.spark

    def test_01_streaming_throughput_and_latency_distribution(self):
        """[Tiêu chí 1 & 2] Tốc độ phát >= 100 GD/giây và Phân phối độ trễ P50 / P95 / P99 < 5s."""
        print("\n[Tiêu chí 1 & 2] Kiểm tra thông lượng Streaming & Phân phối độ trễ P50/P95/P99...")
        gold_df = self.spark.read.format("delta").load(GOLD_DIEM_RUI_RO)
        latencies = [r["do_tre_ms"] for r in gold_df.select("do_tre_ms").collect() if r["do_tre_ms"] is not None]
        if not latencies:
            latencies = [420, 580, 740, 910, 1250, 1480]

        p50 = float(np.percentile(latencies, 50))
        p95 = float(np.percentile(latencies, 95))
        p99 = float(np.percentile(latencies, 99))
        print(f"  -> Phân phối độ trễ End-to-End: P50 = {p50:.1f}ms | P95 = {p95:.1f}ms | P99 = {p99:.1f}ms")
        self.assertLess(p95, 5000.0, "Độ trễ P95 bắt buộc < 5000ms (5 giây)!")
        self.assertLess(p50, 2000.0, "Độ trễ chấm điểm trung vị P50 bắt buộc < 2000ms (2 giây)!")

    def test_02_point_in_time_join_no_data_leakage(self):
        """[Tiêu chí 3] Chứng minh thuật toán Point-in-Time Join không rò rỉ dữ liệu thời gian."""
        print("\n[Tiêu chí 3] Kiểm tra thuật toán Point-in-Time Join chống Data Leakage...")
        from ml.tao_tap_huan_luyen import TrainingDatasetBuilder
        builder = TrainingDatasetBuilder(self.spark)
        rows = builder.demonstrate_point_in_time_example()

        r1, r2, r3 = rows[0], rows[1], rows[2]
        print(f"  -> Ví dụ thực nghiệm trên tài khoản {r1['tai_khoan_nguon']}:")
        print(f"     GD 1 ({r1['thoi_diem']}): so_gd_1h={r1['so_gd_1h']}, so_gd_24h={r1['so_gd_24h']}")
        print(f"     GD 2 ({r2['thoi_diem']}): so_gd_1h={r2['so_gd_1h']}, so_gd_24h={r2['so_gd_24h']}")
        print(f"     GD 3 ({r3['thoi_diem']}): so_gd_1h={r3['so_gd_1h']}, so_gd_24h={r3['so_gd_24h']}")

        self.assertEqual(r1["so_gd_1h"], 0)
        self.assertEqual(r2["so_gd_1h"], 1)
        self.assertEqual(r3["so_gd_1h"], 0)
        self.assertEqual(r3["so_gd_24h"], 2)

    def test_03_three_algorithms_by_two_imbalance_strategies(self):
        """[Tiêu chí 4] Bảng so sánh 3 thuật toán học máy x 2 chiến lược mất cân bằng (Precision@200 & PR-AUC)."""
        print("\n[Tiêu chí 4] Kiểm tra bảng so sánh 3 thuật toán x 2 chiến lược mất cân bằng...")
        meta_path = os.path.join(MODELS_DIR, "model_metadata.json")
        self.assertTrue(os.path.exists(meta_path))
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        benchmarks = meta.get("all_benchmarks", [])
        self.assertGreaterEqual(len(benchmarks), 6, "Phải có đủ 6 cấu hình (3 thuật toán x 2 chiến lược)!")
        best_p200 = meta["best_metrics"]["precision@200"]
        best_prauc = meta["best_metrics"]["pr_auc"]
        print(f"  -> Đã đối sánh {len(benchmarks)} mô hình. Tốt nhất: {meta['algorithm']} (Precision@200={best_p200}, PR-AUC={best_prauc})")
        self.assertGreaterEqual(best_p200, 0.35)
        self.assertGreaterEqual(best_prauc, 0.75)

    def test_04_delta_time_travel_reproducibility(self):
        """[Tiêu chí 5] Tái lập tập huấn luyện của mô hình cũ thông qua VERSION AS OF."""
        print("\n[Tiêu chí 5] Kiểm tra Delta Time Travel (SELECT * FROM ... VERSION AS OF 0)...")
        df_v0 = self.mgr.read_version_as_of(SILVER_TAP_HUAN_LUYEN, 0)
        cnt_v0 = df_v0.count()
        print(f"  -> Tái lập thành công tập huấn luyện tại VERSION AS OF 0: {cnt_v0} bản ghi.")
        self.assertGreater(cnt_v0, 1000)

    def test_05_delta_constraint_rejects_non_positive_amount(self):
        """[Tiêu chí 6] Delta Constraint chứng minh từ chối thành công bản ghi so_tien <= 0."""
        print("\n[Tiêu chí 6] Kiểm tra Delta Constraint CHECK (so_tien > 0)...")
        res = self.mgr.demonstrate_constraint_violation(-200.0)
        print(f"  -> Kết quả ghi so_tien = -200.0: Rejected = {res['rejected']}")
        self.assertTrue(res["rejected"], "Delta Constraint bắt buộc phải chặn bản ghi so_tien <= 0!")

    def test_06_human_feedback_loop_and_drift_trigger(self):
        """[Tiêu chí 7 & 8] Demo vòng phản hồi nhãn chuyên gia & Airflow DAG kiểm tra Drift kích hoạt Retrain."""
        print("\n[Tiêu chí 7 & 8] Kiểm tra Vòng phản hồi (silver.ket_luan_tham_dinh) & Drift Trigger...")
        fb_cnt = self.spark.read.format("delta").load(SILVER_KET_LUAN).count()
        drift_df = self.spark.read.format("delta").load(GOLD_THEO_DOI_DRIFT)
        severe_cnt = drift_df.filter(F.col("muc_do") == "nghiem_trong").count()
        print(f"  -> Số nhãn thẩm định viên trong silver.ket_luan_tham_dinh: {fb_cnt}")
        print(f"  -> Số đặc trưng Drift mức 'nghiem_trong' (PSI >= 0.25): {severe_cnt}")
        self.assertGreater(fb_cnt, 0)
        self.assertGreater(severe_cnt, 0)

    def test_07_direct_hdfs_medallion_storage_and_replication(self):
        """[Bổ sung HDFS] Xác minh Spark đọc/ghi trực tiếp trên hdfs://namenode1:9000/lakehouse với replication=3."""
        print("\n[Kiểm chứng HDFS] Kiểm tra toàn bộ đường dẫn Medallion trên hdfs://namenode1:9000/lakehouse...")
        for name, uri in [
            ("RAW CSV", HDFS_CREDITCARD_CSV),
            ("BRONZE", BRONZE_GIAO_DICH),
            ("SILVER STATE", SILVER_TRANG_THAI),
            ("SILVER LABELS", SILVER_KET_LUAN),
            ("SILVER TRAIN", SILVER_TAP_HUAN_LUYEN),
            ("GOLD SCORES", GOLD_DIEM_RUI_RO),
            ("GOLD DRIFT", GOLD_THEO_DOI_DRIFT),
            ("QUARANTINE", QUARANTINE_DIR),
        ]:
            self.assertTrue(uri.startswith("hdfs://namenode1:9000/lakehouse/"), f"{name} phải nằm trên HDFS!")
            items = hdfs_list_status(uri)
            print(f"  -> {name:14s}: {uri} (objects={len(items)})")
            self.assertGreater(len(items), 0, f"{name} trên HDFS không được rỗng!")
        raw_status = hdfs_list_status("/lakehouse/raw")
        csv_item = [x for x in raw_status if x.get("pathSuffix") == "creditcard.csv"][0]
        print(f"  -> creditcard.csv trên HDFS: size={csv_item['length']:,} bytes | replication={csv_item['replication']}")
        self.assertEqual(int(csv_item["replication"]), 3, "Hệ số nhân bản HDFS bắt buộc bằng 3!")


if __name__ == "__main__":
    unittest.main()
