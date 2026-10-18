import os
import sys
import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    class DAG:
        def __init__(self, *args, **kwargs): pass
    class PythonOperator:
        def __init__(self, *args, **kwargs): pass
        def __rshift__(self, other): return other

from config.settings import (
    BRONZE_GIAO_DICH, SILVER_TRANG_THAI, GOLD_DIEM_RUI_RO, log_pipeline_event
)

default_args = {
    "owner": "bd02_fraud_team",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag = DAG(
    "gl_bao_tri",
    default_args=default_args,
    description="DAG 4: Bảo trì định kỳ Thứ Tư 03:00 (0 3 * * 3) - OPTIMIZE, ZORDER, VACUUM & Xóa state > 30 ngày",
    schedule="0 3 * * 3",
    catchup=False,
)


def run_optimize_and_zorder(**context):
    from core_delta.table_manager import DeltaTableManager
    mgr = DeltaTableManager()
    res = mgr.optimize_and_zorder(BRONZE_GIAO_DICH, ["tai_khoan_nguon", "thoi_diem"])
    return res


def run_vacuum(**context):
    from core_delta.table_manager import DeltaTableManager
    mgr = DeltaTableManager()
    mgr.vacuum_table(BRONZE_GIAO_DICH, retention_hours=168)
    mgr.vacuum_table(GOLD_DIEM_RUI_RO, retention_hours=168)
    return "VACUUM_COMPLETED"


def cleanup_inactive_state(**context):
    from core_delta.table_manager import DeltaTableManager
    from pyspark.sql import functions as F
    mgr = DeltaTableManager()
    silver_delta = mgr.get_delta_table(SILVER_TRANG_THAI)
    cutoff = datetime.datetime.now() - datetime.timedelta(days=30)
    silver_delta.delete(condition=F.col("lan_gd_gan_nhat") < F.lit(cutoff))
    log_pipeline_event(
        "Airflow DAG 4: gl_bao_tri",
        "silver.trang_thai_khach_hang & bronze.giao_dich",
        "Hoàn tất OPTIMIZE ZORDER BY (tai_khoan_nguon, thoi_diem), VACUUM & dọn state > 30 ngày",
        "Đảm bảo hiệu năng đọc/ghi Lakehouse ổn định dài hạn",
        count=1
    )
    return "STATE_CLEANUP_COMPLETED"


t_opt = PythonOperator(task_id="optimize_va_zorder", python_callable=run_optimize_and_zorder, dag=dag)
t_vac = PythonOperator(task_id="vacuum_delta_tables", python_callable=run_vacuum, dag=dag)
t_clean = PythonOperator(task_id="xoa_state_khong_hoat_dong_30d", python_callable=cleanup_inactive_state, dag=dag)

t_opt >> t_vac >> t_clean

if __name__ == "__main__":
    print("Kết quả OPTIMIZE ZORDER:", run_optimize_and_zorder())
    print("Kết quả VACUUM:", run_vacuum())
    print("Kết quả Cleanup State:", cleanup_inactive_state())
