import io
import os
import sys
import json
import socket
import datetime
import urllib.request
from urllib.parse import unquote

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator, BranchPythonOperator
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
except ImportError:
    class DAG:
        def __init__(self, *args, **kwargs): pass
    class PythonOperator:
        def __init__(self, *args, **kwargs): pass
        def __rshift__(self, other): return other
    class BranchPythonOperator(PythonOperator): pass
    class BashOperator(PythonOperator): pass
    class EmptyOperator(PythonOperator): pass

from config.settings import (
    IS_DOCKER, BRONZE_GIAO_DICH, KAFKA_BOOTSTRAP_SERVERS,
    hdfs_list_status, hdfs_read_bytes, log_pipeline_event
)

default_args = {
    "owner": "bd02_fraud_team",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
}

dag = DAG(
    "gl_giam_sat_streaming",
    default_args=default_args,
    description="DAG 1: Giám sát Spark Streaming REST API, Kafka Broker & HDFS Bronze định kỳ 5 phút (*/5 * * * *)",
    schedule="*/5 * * * *",
    catchup=False,
)


def _count_hdfs_delta_rows(table_uri):
    """Reads active Parquet files from HDFS _delta_log via WebHDFS when running inside Airflow container."""
    import pandas as pd
    statuses = hdfs_list_status(f"{table_uri.rstrip('/')}/_delta_log")
    commits = sorted(
        st["pathSuffix"] for st in statuses
        if st.get("type") == "FILE" and st.get("pathSuffix", "").endswith(".json")
        and st.get("pathSuffix", "").replace(".json", "").isdigit()
    )
    active_files = []
    active_set = set()
    for fname in commits:
        raw = hdfs_read_bytes(f"{table_uri.rstrip('/')}/_delta_log/{fname}").decode("utf-8")
        for line in raw.splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            if "add" in obj and "path" in obj["add"]:
                p = obj["add"]["path"]
                if p not in active_set:
                    active_set.add(p)
                    active_files.append(p)
            elif "remove" in obj and "path" in obj["remove"]:
                p = obj["remove"]["path"]
                if p in active_set:
                    active_set.discard(p)
                    active_files = [x for x in active_files if x != p]

    total_rows = 0
    recent_rows = 0
    cutoff_10m = pd.Timestamp.now() - pd.Timedelta(minutes=10)
    for rel in active_files:
        pq_bytes = hdfs_read_bytes(f"{table_uri.rstrip('/')}/{unquote(rel).lstrip('/')}")
        df = pd.read_parquet(io.BytesIO(pq_bytes))
        total_rows += len(df)
        if "thoi_diem_nhan" in df.columns:
            ts_col = pd.to_datetime(df["thoi_diem_nhan"], errors="coerce")
            recent_rows += int((ts_col >= cutoff_10m).sum())
    return total_rows, recent_rows, len(commits)


def check_streaming_health_and_branch(**context):
    """Kiểm tra:
    1. Spark REST API (http://localhost:4040/api/v1/applications hoặc host.docker.internal:4040)
    2. Kafka Broker TCP (kafka:9092 / localhost:9092)
    3. Bảng Delta Lake trên HDFS: hdfs://namenode1:9000/lakehouse/bronze/giao_dich
    """
    spark_urls = [
        "http://host.docker.internal:4040/api/v1/applications",
        "http://localhost:4040/api/v1/applications",
    ]
    spark_rest_ok = False
    spark_app_info = "Embedded/Standby"
    for u in spark_urls:
        try:
            with urllib.request.urlopen(u, timeout=1.5) as r:
                if r.status == 200:
                    apps = json.loads(r.read().decode("utf-8"))
                    spark_rest_ok = True
                    spark_app_info = apps[0].get("name", "RealTimeScoringEngine") if apps else "Active"
                    break
        except Exception:
            pass

    host_k, port_k = KAFKA_BOOTSTRAP_SERVERS.split(":")
    kafka_ok = False
    s = socket.socket()
    s.settimeout(2.0)
    try:
        s.connect((host_k, int(port_k)))
        kafka_ok = True
    except Exception:
        kafka_ok = False
    finally:
        s.close()

    total_bronze = 0
    recent_10m_count = 0
    commits_cnt = 0
    try:
        if not IS_DOCKER:
            from core_delta.table_manager import DeltaTableManager
            from pyspark.sql import functions as F
            mgr = DeltaTableManager()
            df_b = mgr.spark.read.format("delta").load(BRONZE_GIAO_DICH)
            total_bronze = df_b.count()
            cutoff_10m = datetime.datetime.now() - datetime.timedelta(minutes=10)
            recent_10m_count = df_b.filter(F.col("thoi_diem_nhan") >= F.lit(cutoff_10m)).count()
            commits_cnt = mgr.get_history(BRONZE_GIAO_DICH).count()
        else:
            total_bronze, recent_10m_count, commits_cnt = _count_hdfs_delta_rows(BRONZE_GIAO_DICH)
    except Exception as e:
        print(f"[DAG 1 Warning] Loi doc HDFS Bronze: {e}")

    is_healthy = kafka_ok and total_bronze > 0
    status_str = "HEALTHY" if is_healthy else "RESTART_REQUIRED"

    print("=" * 75)
    print(f"[Airflow DAG 1: gl_giam_sat_streaming] KET QUA GIAM SAT HE THONG")
    print(f"  - Kafka Broker ({KAFKA_BOOTSTRAP_SERVERS}) : {'ONLINE' if kafka_ok else 'OFFLINE'}")
    print(f"  - Spark Streaming UI (:4040)              : {spark_app_info} (REST={spark_rest_ok})")
    print(f"  - HDFS Delta Table ({BRONZE_GIAO_DICH}) : {total_bronze} rows | {commits_cnt} commits")
    print(f"  - Giao dich moi trong 10 phut qua         : {recent_10m_count} rows")
    print(f"  - Quyet dinh nhanh (BranchPythonOperator) : {status_str}")
    print("=" * 75)

    log_pipeline_event(
        "Airflow DAG 1: gl_giam_sat_streaming",
        "Spark Streaming & HDFS Bronze Monitor",
        f"Kiểm tra định kỳ */5 phút: Kafka={'ONLINE' if kafka_ok else 'OFFLINE'}, HDFS Bronze={total_bronze} GD ({commits_cnt} commits)",
        f"Trạng thái: {status_str} | Mới 10p: {recent_10m_count} GD",
        count=total_bronze
    )

    if is_healthy:
        return "ghi_nhat_ky_streaming_on_dinh"
    return "canh_bao_va_khoi_dong_lai_spark_submit"


t_check = BranchPythonOperator(
    task_id="kiem_tra_spark_rest_kafka_lag_bronze_10m",
    python_callable=check_streaming_health_and_branch,
    dag=dag,
)

t_ok = EmptyOperator(
    task_id="ghi_nhat_ky_streaming_on_dinh",
    dag=dag,
)

t_restart = BashOperator(
    task_id="canh_bao_va_khoi_dong_lai_spark_submit",
    bash_command='echo "Canh bao: Khoi dong lai Spark Streaming cham_diem.py"',
    dag=dag,
)

t_check >> [t_ok, t_restart]

if __name__ == "__main__":
    print("Kết quả chạy DAG 1 (gl_giam_sat_streaming):", check_streaming_health_and_branch())
