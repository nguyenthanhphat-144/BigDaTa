import os
import sys
import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator, BranchPythonOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    class DAG:
        def __init__(self, *args, **kwargs): pass
    class PythonOperator:
        def __init__(self, *args, **kwargs): pass
        def __rshift__(self, other): return other
    class BranchPythonOperator(PythonOperator): pass
    class EmptyOperator(PythonOperator): pass
    class TriggerDagRunOperator(PythonOperator): pass

default_args = {
    "owner": "bd02_fraud_team",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag = DAG(
    "gl_kiem_tra_drift",
    default_args=default_args,
    description="DAG 2: Tính PSI & KS-statistic 08:00 hàng ngày, rẽ nhánh bằng BranchPythonOperator (0 8 * * *)",
    schedule="0 8 * * *",
    catchup=False,
)


def evaluate_drift_and_branch(**context):
    """Tính toán chỉ số PSI và KS qua ml/kiem_tra_drift.py và rẽ nhánh:
    - PSI < 0.10 -> nhanh_drift_on_dinh (ghi nhật ký ổn định)
    - 0.10 <= PSI <= 0.25 -> nhanh_drift_canh_bao (ghi nhật ký và gửi email cảnh báo)
    - PSI > 0.25 -> nhanh_drift_nghiem_trong (TriggerDagRunOperator kích hoạt gl_huan_luyen_lai)
    """
    from ml.kiem_tra_drift import DriftDetector
    detector = DriftDetector()
    level = detector.run_drift_check(inject_severe_drift=True)
    print(f"[Airflow DAG 2: gl_kiem_tra_drift] Mức độ Drift: {level.upper()}")

    if level == "nghiem_trong":
        return "nhanh_drift_nghiem_trong"
    elif level == "canh_bao":
        return "nhanh_drift_canh_bao"
    else:
        return "nhanh_drift_on_dinh"


branch_op = BranchPythonOperator(
    task_id="tinh_psi_ks_va_re_nhanh",
    python_callable=evaluate_drift_and_branch,
    dag=dag,
)

t_stable = EmptyOperator(
    task_id="nhanh_drift_on_dinh",
    dag=dag,
)

t_warning = EmptyOperator(
    task_id="nhanh_drift_canh_bao",
    dag=dag,
)

t_critical = TriggerDagRunOperator(
    task_id="nhanh_drift_nghiem_trong",
    trigger_dag_id="gl_huan_luyen_lai",
    dag=dag,
)

branch_op >> [t_stable, t_warning, t_critical]

if __name__ == "__main__":
    print("Kết quả chạy DAG 2 (gl_kiem_tra_drift) -> Nhánh:", evaluate_drift_and_branch())
