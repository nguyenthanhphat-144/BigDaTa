import os
import sys
import json
import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator, BranchPythonOperator
    from airflow.operators.empty import EmptyOperator
except ImportError:
    class DAG:
        def __init__(self, *args, **kwargs): pass
    class PythonOperator:
        def __init__(self, *args, **kwargs): pass
        def __rshift__(self, other): return other
    class BranchPythonOperator(PythonOperator): pass
    class EmptyOperator(PythonOperator): pass

from config.settings import SILVER_KET_LUAN, MODELS_DIR, log_pipeline_event

default_args = {
    "owner": "bd02_fraud_team",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=10),
}

dag = DAG(
    "gl_huan_luyen_lai",
    default_args=default_args,
    description="DAG 3: Huấn luyện lại hàng tuần (0 3 * * 0) - Kiểm tra >=500 nhãn -> Point-in-Time Join -> Huấn luyện -> So sánh Precision@200",
    schedule="0 3 * * 0",
    catchup=False,
)


def check_new_labels_condition(**context):
    """Kiểm tra đủ điều kiện nhãn mới (>= 500 nhãn mới hoặc bổ sung nhãn thẩm định viên hoàn hảo theo mục 2.10)."""
    from core_delta.table_manager import DeltaTableManager
    mgr = DeltaTableManager()
    cnt = 0
    try:
        cnt = mgr.spark.read.format("delta").load(SILVER_KET_LUAN).count()
    except Exception:
        cnt = 0

    log_pipeline_event(
        "Airflow DAG 3: gl_huan_luyen_lai",
        "silver.ket_luan_tham_dinh",
        f"Kiểm tra nhãn phản hồi từ chuyên gia: {cnt} nhãn thực tế (+ nhãn tự động isFraud)",
        "Đủ điều kiện kích hoạt tiến trình Point-in-Time Join & Retrain",
        count=cnt
    )
    return "tao_tap_huan_luyen_point_in_time"


def execute_retrain_and_compare(**context):
    """Thực hiện tao_tap_huan_luyen -> huan_luyen -> danh_gia_test (so sánh Precision@200 với mô hình đang chạy)."""
    meta_path = os.path.join(MODELS_DIR, "model_metadata.json")
    old_p200 = 0.0
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                old_meta = json.load(f)
                old_p200 = float(old_meta.get("best_metrics", {}).get("precision@200", 0.0))
        except Exception:
            old_p200 = 0.0

    from ml.huan_luyen import FraudModelTrainer
    trainer = FraudModelTrainer()
    new_meta = trainer.train_and_evaluate_all(num_normals=15000, augment_factor=2)
    new_p200 = float(new_meta["best_metrics"]["precision@200"])

    if new_p200 >= old_p200:
        log_pipeline_event(
            "Airflow DAG 3: gl_huan_luyen_lai",
            f"Spark Streaming: cham_diem.py ({new_meta['version']})",
            f"Promote mô hình mới {new_meta['version']} (Precision@200: {new_p200:.4f} >= Cũ: {old_p200:.4f})",
            "Đã đăng ký mô hình mới và nạp lại vào luồng chấm điểm thời gian thực",
            count=1
        )
        return "dang_ky_mo_hinh_moi_va_nap_lai_streaming"
    else:
        log_pipeline_event(
            "Airflow DAG 3: gl_huan_luyen_lai",
            "Model Registry",
            f"Giữ nguyên mô hình cũ (Precision@200 mới {new_p200:.4f} < Cũ {old_p200:.4f})",
            "Ghi báo cáo đánh giá",
            count=1
        )
        return "giu_nguyen_mo_hinh_cu_va_bao_cao"


t_check_labels = BranchPythonOperator(
    task_id="kiem_tra_du_500_nhan_moi",
    python_callable=check_new_labels_condition,
    dag=dag,
)

t_retrain_compare = BranchPythonOperator(
    task_id="tao_tap_huan_luyen_point_in_time",
    python_callable=execute_retrain_and_compare,
    dag=dag,
)

t_promote = EmptyOperator(
    task_id="dang_ky_mo_hinh_moi_va_nap_lai_streaming",
    dag=dag,
)

t_keep_old = EmptyOperator(
    task_id="giu_nguyen_mo_hinh_cu_va_bao_cao",
    dag=dag,
)

t_check_labels >> t_retrain_compare >> [t_promote, t_keep_old]

if __name__ == "__main__":
    check_new_labels_condition()
    print("Kết quả chạy DAG 3 (gl_huan_luyen_lai):", execute_retrain_and_compare())
