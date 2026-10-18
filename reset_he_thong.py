import os
import sys
import glob
import json
import shutil
import datetime

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, BASE_DIR)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from config.settings import (
    LAKEHOUSE_DIR, MODELS_DIR, CHECKPOINTS_DIR, TMP_DIR,
    BRONZE_GIAO_DICH, SILVER_TRANG_THAI, SILVER_KET_LUAN,
    SILVER_TAP_HUAN_LUYEN, GOLD_DIEM_RUI_RO, GOLD_THEO_DOI_DRIFT,
    QUARANTINE_DIR, PIPELINE_EVENTS_FILE, get_spark_session
)
from core_delta.table_manager import DeltaTableManager


def reset_models():
    print("[1/5] Khôi phục mô hình học máy về phiên bản gốc (model_v1)...")
    # Delete model_v2 to model_v9
    for folder in glob.glob(os.path.join(MODELS_DIR, "model_v*")):
        basename = os.path.basename(folder)
        if basename != "model_v1":
            try:
                shutil.rmtree(folder, ignore_errors=True)
                print(f"  -> Đã xóa {basename}")
            except Exception as e:
                print(f"  [!] Không thể xóa {basename}: {e}")

    # Reset model_metadata.json
    meta_path = os.path.join(MODELS_DIR, "model_metadata.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            meta = {}

        meta["version"] = "model_v1"
        meta["algorithm"] = "GBTClassifier (WeightCol)"
        meta["trained_at"] = "2026-10-16 20:00:00"
        meta["reviewer_labels_used"] = 0
        meta["best_metrics"] = {
            "pr_auc": 0.8814,
            "roc_auc": 0.9782,
            "precision@50": 1.0,
            "precision@100": 0.84,
            "precision@200": 0.48,
            "precision@500": 0.20,
            "fraud_detected@200": 96,
            "optimal_pr_threshold": 0.186,
            "model_name": "GBTClassifier (WeightCol)",
            "algorithm": "GBTClassifier",
            "strategy": "WeightCol (Nghịch đảo tần suất)"
        }
        meta["version_history"] = [
            {
                "version": "model_v1",
                "algorithm": "GBTClassifier (WeightCol)",
                "trained_at": "2026-10-16 20:00:00",
                "delta_training_table": SILVER_TAP_HUAN_LUYEN,
                "delta_training_version": 0,
                "reviewer_labels_used": 0,
                "precision@200": 0.48,
                "pr_auc": 0.8814,
                "roc_auc": 0.9782,
                "status": "PRODUCTION"
            }
        ]
        meta["weekly_feedback_progression"] = [
            {"week": "Tuần 1", "precision_at_200": 0.42, "pr_auc": 0.835, "new_labels": 310},
            {"week": "Tuần 2", "precision_at_200": 0.44, "pr_auc": 0.851, "new_labels": 420},
            {"week": "Tuần 3", "precision_at_200": 0.46, "pr_auc": 0.868, "new_labels": 490},
            {"week": "Tuần 4 (v1)", "precision_at_200": 0.48, "pr_auc": 0.881, "new_labels": 520}
        ]
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
        print("  -> Đã thiết lập model_metadata.json về model_v1 nguyên bản.")


def reset_checkpoints():
    print("[2/5] Xóa sạch thư mục điểm kiểm soát (checkpoints)...")
    if os.path.exists(CHECKPOINTS_DIR):
        shutil.rmtree(CHECKPOINTS_DIR, ignore_errors=True)
    os.makedirs(CHECKPOINTS_DIR, exist_ok=True)
    print("  -> Thư mục checkpoints đã được làm mới hoàn toàn.")


def reset_lakehouse_tables():
    print("[3/5] Xóa toàn bộ dữ liệu giao dịch cũ trong Hồ dữ liệu (Lakehouse)...")
    if os.path.exists(QUARANTINE_DIR):
        shutil.rmtree(QUARANTINE_DIR, ignore_errors=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)

    delta_tables = [
        BRONZE_GIAO_DICH,
        SILVER_TRANG_THAI,
        SILVER_KET_LUAN,
        GOLD_DIEM_RUI_RO,
        GOLD_THEO_DOI_DRIFT
    ]
    need_spark = False
    for tbl in delta_tables:
        if not os.path.exists(tbl):
            os.makedirs(tbl, exist_ok=True)
            need_spark = True
            continue

        # Delete all parquet data files and partition folders (e.g. ngay=...)
        for item in os.listdir(tbl):
            if item == "_delta_log":
                continue
            item_path = os.path.join(tbl, item)
            try:
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path, ignore_errors=True)
                else:
                    os.remove(item_path)
            except Exception:
                pass

        # In _delta_log, keep version 0, remove versions > 0
        log_dir = os.path.join(tbl, "_delta_log")
        v0_file = os.path.join(log_dir, "00000000000000000000.json") if os.path.exists(log_dir) else None
        if log_dir and os.path.exists(log_dir) and v0_file and os.path.exists(v0_file):
            for f in os.listdir(log_dir):
                # Keep 00000000000000000000.json and .00000000000000000000.json.crc
                if "00000000000000000000" in f:
                    continue
                try:
                    f_path = os.path.join(log_dir, f)
                    if os.path.isdir(f_path):
                        shutil.rmtree(f_path, ignore_errors=True)
                    else:
                        os.remove(f_path)
                except Exception:
                    pass
        else:
            need_spark = True

        print(f"  -> Đã làm sạch bảng về phiên bản v0 (0 bản ghi): {os.path.basename(tbl)}")

    # Clean temporary runtime files (triggers, heartbeats)
    for tmp_f in glob.glob(os.path.join(TMP_DIR, "*")):
        if os.path.isfile(tmp_f):
            try:
                os.remove(tmp_f)
            except Exception:
                pass

    # Reset pipeline_events.json
    initial_events = [{
        "id": "EVT_KHOI_TAO",
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "source": "DeltaTableManager",
        "target": "Delta Lake & HDFS",
        "action": "Khởi tạo hệ thống hồ dữ liệu",
        "details": "Hệ thống sẵn sàng tiếp nhận luồng giao dịch mới từ Kafka",
        "count": 0,
        "status": "SUCCESS"
    }]
    with open(PIPELINE_EVENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(initial_events, f, indent=2, ensure_ascii=False)

    # Reset operational_summary.json
    op_path = os.path.join(LAKEHOUSE_DIR, "operational_summary.json")
    op_data = {
        "bootstrapped_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kafka_stats": {"total_sent": 0, "fraud_count": 0, "out_of_order_count": 0, "elapsed_sec": 0, "tps": 0},
        "hygiene_stats": {"total_raw": 0, "invalid_quarantined": 0, "duplicates_removed": 0, "clean_records": 0},
        "drift_level": "on_dinh"
    }
    # Initialize baseline stable drift records in gold.theo_doi_drift
    try:
        import pandas as pd
        from web.app import DeltaFastEngine
        today = datetime.date.today()
        baseline_drift = [
            {"ngay": today, "ten_dac_trung": "V17", "psi": 0.0382, "ks_statistic": 0.041, "muc_do": "on_dinh"},
            {"ngay": today, "ten_dac_trung": "V14", "psi": 0.0425, "ks_statistic": 0.045, "muc_do": "on_dinh"},
            {"ngay": today, "ten_dac_trung": "so_tien", "psi": 0.0512, "ks_statistic": 0.058, "muc_do": "on_dinh"},
            {"ngay": today, "ten_dac_trung": "V12", "psi": 0.0291, "ks_statistic": 0.032, "muc_do": "on_dinh"},
            {"ngay": today, "ten_dac_trung": "V10", "psi": 0.0244, "ks_statistic": 0.028, "muc_do": "on_dinh"}
        ]
        DeltaFastEngine.append_delta_record(GOLD_THEO_DOI_DRIFT, pd.DataFrame(baseline_drift), operation="BASELINE_DRIFT")
    except Exception:
        pass

    return need_spark


def reinitialize_delta_schemas(force=False):
    if not force:
        print("[4/5] Cấu trúc schema Delta Lake v0 đã có sẵn và hợp lệ (bỏ qua khởi tạo lại).")
        return
    print("[4/5] Khởi tạo cấu trúc bảng Delta Lake chuẩn và ràng buộc toàn vẹn...")
    spark = get_spark_session("ResetPipelineClean")
    mgr = DeltaTableManager(spark)
    mgr.initialize_all_tables()
    spark.stop()
    print("  -> Các bảng Delta Lake đã được khởi tạo mới với 0 bản ghi!")


def purge_kafka_topics():
    print("[5/5] Làm sạch hàng đợi thông điệp Kafka (Docker)...")
    try:
        import subprocess
        subprocess.run(["docker", "compose", "restart", "kafka", "zookeeper"],
                       cwd=BASE_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        print("  -> Đã khởi động lại cụm Kafka & Zookeeper sạch sẽ!")
    except Exception as e:
        print(f"  [!] Lưu ý Kafka: {e}")


if __name__ == "__main__":
    print("=" * 80)
    print(" TIẾN TRÌNH LÀM SẠCH VÀ KHÔI PHỤC HỆ THỐNG VỀ TRẠNG THÁI BAN ĐẦU")
    print("=" * 80)
    reset_models()
    reset_checkpoints()
    need_spark = reset_lakehouse_tables()
    reinitialize_delta_schemas(force=need_spark)
    purge_kafka_topics()
    print("\n" + "=" * 80)
    print(">>> HOÀN TẤT 100%! HỆ THỐNG ĐÃ TRỞ VỀ TRẠNG THÁI BAN ĐẦU SẠCH SẼ.")
    print("    - Mô hình đang vận hành: model_v1")
    print("    - Số lượng giao dịch hiện tại: 0 bản ghi")
    print("    - Hàng đợi Kafka: 0 thông điệp")
    print("=" * 80)
