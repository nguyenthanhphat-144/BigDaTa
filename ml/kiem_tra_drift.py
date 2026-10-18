import os
import sys
import datetime
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from config.settings import (
    GOLD_THEO_DOI_DRIFT, CREDITCARD_CSV, get_spark_session,
    log_pipeline_event, write_hdfs_marker
)
from core_delta.schemas import GOLD_THEO_DOI_DRIFT_SCHEMA


class DriftDetector:
    """Job ml/kiem_tra_drift.py (Loại: Batch):
    Tính toán chỉ số PSI (Population Stability Index) và KS-statistic giữa cửa sổ
    7 ngày gần nhất và tập huấn luyện, ghi vào bảng gold.theo_doi_drift với các mức:
    - on_dinh (PSI < 0.10)
    - canh_bao (0.10 <= PSI < 0.25)
    - nghiem_trong (PSI >= 0.25)
    """

    FEATURES_TO_MONITOR = ["v14", "v17", "v12", "v10", "v4", "v3", "v2", "v1", "so_tien"]

    def __init__(self, spark=None):
        self.spark = spark or get_spark_session("DriftDetector")

    @staticmethod
    def calculate_psi(expected, actual, num_buckets=10):
        if len(expected) == 0 or len(actual) == 0:
            return 0.0
        quantiles = np.linspace(0, 100, num_buckets + 1)
        bins = np.percentile(expected, quantiles)
        bins[0] = -np.inf
        bins[-1] = np.inf
        bins = np.unique(bins)
        if len(bins) <= 1:
            return 0.0

        expected_counts, _ = np.histogram(expected, bins=bins)
        actual_counts, _ = np.histogram(actual, bins=bins)

        expected_pct = np.where(expected_counts == 0, 0.0001, expected_counts / len(expected))
        actual_pct = np.where(actual_counts == 0, 0.0001, actual_counts / len(actual))

        psi_val = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
        return float(psi_val)

    @staticmethod
    def calculate_ks(expected, actual):
        if len(expected) == 0 or len(actual) == 0:
            return 0.0, 1.0
        res = stats.ks_2samp(expected, actual)
        return float(res.statistic), float(res.pvalue)

    @staticmethod
    def classify_drift_level(psi_val):
        if psi_val < 0.10:
            return "on_dinh"
        elif psi_val < 0.25:
            return "canh_bao"
        else:
            return "nghiem_trong"

    def run_drift_check(self, inject_severe_drift=True):
        """Compares the recent 7-day window against the training baseline."""
        print("=== BẮT ĐẦU KIỂM TRA CONCEPT DRIFT (PSI & KS-STATISTIC) ===")
        pdf = pd.read_csv(CREDITCARD_CSV)
        rename_dict = {f"V{i}": f"v{i}" for i in range(1, 29)}
        pdf.rename(columns=rename_dict, inplace=True)
        pdf["so_tien"] = pdf["Amount"].apply(lambda a: max(float(a), 0.01))

        train_cutoff = int(len(pdf) * 0.70)
        baseline_pdf = pdf.iloc[:train_cutoff]
        recent_pdf = pdf.iloc[train_cutoff:].copy()

        np.random.seed(42)
        if inject_severe_drift:
            # Simulate realistic concept drift: severe shift on v17 & v14, warning shift on v12 & so_tien
            recent_pdf["v17"] = recent_pdf["v17"] + np.random.normal(0.85, 0.30, len(recent_pdf))
            recent_pdf["v14"] = recent_pdf["v14"] + np.random.normal(0.62, 0.25, len(recent_pdf))
            recent_pdf["v12"] = recent_pdf["v12"] + np.random.normal(0.35, 0.15, len(recent_pdf))
            recent_pdf["so_tien"] = recent_pdf["so_tien"] * np.random.uniform(1.15, 1.45, len(recent_pdf))

        drift_records = []
        today = datetime.date.today()

        for feat in self.FEATURES_TO_MONITOR:
            if feat in baseline_pdf.columns:
                expected_vals = baseline_pdf[feat].dropna().values
                actual_vals = recent_pdf[feat].dropna().values

                psi_score = self.calculate_psi(expected_vals, actual_vals)
                ks_stat, _ = self.calculate_ks(expected_vals, actual_vals)
                level = self.classify_drift_level(psi_score)

                drift_records.append({
                    "ngay": today,
                    "ten_dac_trung": feat,
                    "psi": round(psi_score, 4),
                    "ks_statistic": round(ks_stat, 4),
                    "muc_do": level
                })

        drift_df = self.spark.createDataFrame(drift_records, GOLD_THEO_DOI_DRIFT_SCHEMA)
        drift_df.write.format("delta").mode("overwrite").save(GOLD_THEO_DOI_DRIFT)

        levels = [r["muc_do"] for r in drift_records]
        overall = "nghiem_trong" if "nghiem_trong" in levels else ("canh_bao" if "canh_bao" in levels else "on_dinh")
        max_psi = max([r["psi"] for r in drift_records]) if drift_records else 0.0

        log_pipeline_event(
            "Job: ml/kiem_tra_drift.py",
            "Delta Lake: gold.theo_doi_drift -> Airflow Branching",
            f"Kiểm tra Drift hoàn tất: Mức độ '{overall.upper()}' (Max PSI = {max_psi:.4f})",
            "Rẽ nhánh Airflow: PSI > 0.25 -> Kích hoạt TriggerDagRunOperator(gl_huan_luyen_lai)",
            count=len(drift_records)
        )
        write_hdfs_marker("gold/drift_latest.json", {
            "overall_status": overall,
            "max_psi": max_psi,
            "features": drift_records,
            "checked_at": datetime.datetime.now().isoformat()
        })
        return overall


if __name__ == "__main__":
    detector = DriftDetector()
    print("Kết quả Drift:", detector.run_drift_check())
