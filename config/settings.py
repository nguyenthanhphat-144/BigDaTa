import os
import sys
import json
import time
import socket
import datetime
import subprocess
import urllib.request

# Automatically resolve Docker container hostnames ('kafka', 'namenode1', 'datanode1..3') to 127.0.0.1 in Python
_orig_getaddrinfo = socket.getaddrinfo
def _patched_getaddrinfo(host, port, *args, **kwargs):
    if host in ("kafka", "namenode1", "datanode1", "datanode2", "datanode3"):
        host = "127.0.0.1"
    return _orig_getaddrinfo(host, port, *args, **kwargs)
socket.getaddrinfo = _patched_getaddrinfo

# Base project directories in d:\BigDaTa2
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LAKEHOUSE_DIR = os.path.join(BASE_DIR, "lakehouse")
MODELS_DIR = os.path.join(BASE_DIR, "models")
CHECKPOINTS_DIR = os.path.join(BASE_DIR, "checkpoints")
HOSTS_FILE = os.path.join(BASE_DIR, "hosts").replace("\\", "/")
PIPELINE_EVENTS_FILE = os.path.join(LAKEHOUSE_DIR, "pipeline_events.json")

# Trusted Python 3.11 / Java 17 / Hadoop winutils environment inside project
VENV_PYTHON = os.path.join(BASE_DIR, "venv311", "Scripts", "python.exe")
JAVA_HOME = os.environ.get("JAVA_HOME", r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot")
HADOOP_HOME = os.path.join(BASE_DIR, "hadoop_home")
TMP_DIR = os.path.join(BASE_DIR, "tmp")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_TRANSACTIONS = "transactions"
KAFKA_TOPIC_ALERTS = "alerts"
KAFKA_CONSUMER_GROUP = "bd02_fraud_scoring_group"

# HDFS Configuration (1 NameNode + 3 DataNodes)
HDFS_NAMENODE_URI = "hdfs://localhost:9000"
HDFS_WEBHDFS_URL = "http://localhost:9870"
HDFS_BASE_DIR = "/bd02_gian_lan"

# Delta Lake Table Paths
BRONZE_GIAO_DICH = os.path.join(LAKEHOUSE_DIR, "bronze", "giao_dich")
SILVER_TRANG_THAI = os.path.join(LAKEHOUSE_DIR, "silver", "trang_thai_khach_hang")
SILVER_KET_LUAN = os.path.join(LAKEHOUSE_DIR, "silver", "ket_luan_tham_dinh")
SILVER_TAP_HUAN_LUYEN = os.path.join(LAKEHOUSE_DIR, "silver", "tap_huan_luyen")
GOLD_DIEM_RUI_RO = os.path.join(LAKEHOUSE_DIR, "gold", "diem_rui_ro")
GOLD_THEO_DOI_DRIFT = os.path.join(LAKEHOUSE_DIR, "gold", "theo_doi_drift")
QUARANTINE_DIR = os.path.join(LAKEHOUSE_DIR, "_quarantine")

# Structured Streaming Checkpoint Paths
CHECKPOINT_SCORING = os.path.join(CHECKPOINTS_DIR, "cham_diem")
CHECKPOINT_STATE = os.path.join(CHECKPOINTS_DIR, "cap_nhat_trang_thai")
CHECKPOINT_BRONZE = os.path.join(CHECKPOINTS_DIR, "bronze_sink")

# Source Dataset in d:\BigDaTa2\creditcard.csv
CREDITCARD_CSV = os.path.join(BASE_DIR, "creditcard.csv")

# Ensure required local directories exist
for d in [
    LAKEHOUSE_DIR, MODELS_DIR, CHECKPOINTS_DIR, QUARANTINE_DIR,
    os.path.join(LAKEHOUSE_DIR, "bronze"),
    os.path.join(LAKEHOUSE_DIR, "silver"),
    os.path.join(LAKEHOUSE_DIR, "gold"),
    TMP_DIR
]:
    os.makedirs(d, exist_ok=True)


def set_console_title(title_text):
    """Sets the Windows PowerShell / CMD window title bar to show the currently running file."""
    try:
        if os.name == "nt":
            import ctypes
            ctypes.windll.kernel32.SetConsoleTitleW(str(title_text))
        sys.stdout.write(f"\x1b]0;{title_text}\x07")
        sys.stdout.flush()
    except Exception:
        pass


def setup_spark_env():
    """Configures environment variables required for PySpark + Delta Lake on Windows."""
    os.environ["TEMP"] = TMP_DIR
    os.environ["TMP"] = TMP_DIR
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    target_py = VENV_PYTHON if os.path.exists(VENV_PYTHON) else sys.executable
    os.environ["PYSPARK_PYTHON"] = target_py
    os.environ["PYSPARK_DRIVER_PYTHON"] = target_py

    hadoop_bin = os.path.join(HADOOP_HOME, "bin")
    java_bin = os.path.join(JAVA_HOME, "bin")
    current_path = os.environ.get("PATH", "")
    if hadoop_bin not in current_path:
        os.environ["PATH"] = f"{hadoop_bin};{java_bin};{current_path}"


def get_spark_session(app_name="BD02_FraudDetection", master="local[*]", ui_port=None):
    """Creates a SparkSession configured with Delta Lake 3.2, Kafka SQL 0-10, and CDF enabled."""
    setup_spark_env()
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    target_py = VENV_PYTHON if os.path.exists(VENV_PYTHON) else sys.executable
    target_py_clean = target_py.replace("\\", "/")

    # Java 17 modularity options to prevent reflection & RPC serialization issues
    java_17_opts = (
        f"-Djdk.net.hosts.file={HOSTS_FILE} "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.pyspark.python", target_py_clean)
        .config("spark.pyspark.driver.python", target_py_clean)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.streaming.checkpointLocation", os.path.join(CHECKPOINTS_DIR, "default"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.warehouse.dir", os.path.join(LAKEHOUSE_DIR, "warehouse"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.storage.blockManagerSlaveTimeoutMs", "800000")
        .config("spark.port.maxRetries", "64")
        .config("spark.driver.extraJavaOptions", java_17_opts)
        .config("spark.executor.extraJavaOptions", java_17_opts)
    )

    if ui_port:
        builder = builder.config("spark.ui.port", str(ui_port))

    spark = configure_spark_with_delta_pip(builder, [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log_pipeline_event(source_node, target_node, action, details="", count=0, status="SUCCESS"):
    """Logs a real-time data flow transition between two system architecture nodes
    so the Flask Web UI can visualize the operating flow step-by-step.
    """
    event = {
        "id": f"EVT_{int(time.time() * 1000)}",
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "source": source_node,
        "target": target_node,
        "action": action,
        "details": details,
        "count": int(count),
        "status": status
    }
    events = []
    if os.path.exists(PIPELINE_EVENTS_FILE):
        try:
            with open(PIPELINE_EVENTS_FILE, "r", encoding="utf-8") as f:
                events = json.load(f)
        except Exception:
            events = []
    events.insert(0, event)
    events = events[:120]  # Keep latest 120 transitions
    try:
        with open(PIPELINE_EVENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2, ensure_ascii=False, default=str)
    except Exception:
        pass
    return event


def write_hdfs_marker(hdfs_subpath, payload_dict):
    """Writes a JSON metadata/audit marker directly into the HDFS cluster (1 NameNode + 3 DataNodes)
    via WebHDFS REST API (or docker exec fallback) with replication=3 across all 3 DataNodes.
    If HDFS is offline, writes instantly to local audit store.
    """
    content_bytes = json.dumps(payload_dict, indent=2, ensure_ascii=False, default=str).encode("utf-8")
    hdfs_full_path = f"{HDFS_BASE_DIR}/{hdfs_subpath.lstrip('/')}"

    # Fast probe: if NameNode port 9870 is not listening, save locally in <1ms
    s = socket.socket()
    s.settimeout(0.08)
    is_up = (s.connect_ex(("127.0.0.1", 9870)) == 0)
    s.close()

    if not is_up:
        local_marker_path = os.path.join(TMP_DIR, "hdfs_markers", hdfs_subpath.replace("/", os.sep))
        try:
            os.makedirs(os.path.dirname(local_marker_path), exist_ok=True)
            with open(local_marker_path, "wb") as f:
                f.write(content_bytes)
        except Exception:
            pass
        return True

    try:
        # Step 1: WebHDFS PUT request
        init_url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{hdfs_full_path}?op=CREATE&overwrite=true&replication=3&noredirect=true"
        req = urllib.request.Request(init_url, method="PUT")
        with urllib.request.urlopen(req, timeout=1.5) as resp:
            loc_data = json.loads(resp.read().decode("utf-8"))
            upload_url = loc_data.get("Location", "")
            for dn_name, host_port in [("datanode1", "9864"), ("datanode2", "9865"), ("datanode3", "9866")]:
                if dn_name in upload_url:
                    upload_url = upload_url.replace(f"{dn_name}:9864", f"localhost:{host_port}")
            req_put = urllib.request.Request(upload_url, data=content_bytes, method="PUT")
            req_put.add_header("Content-Type", "application/octet-stream")
            with urllib.request.urlopen(req_put, timeout=2.0) as put_resp:
                return True
    except Exception:
        try:
            tmp_file = os.path.join(TMP_DIR, "hdfs_marker_tmp.json")
            with open(tmp_file, "w", encoding="utf-8") as f:
                f.write(content_bytes.decode("utf-8"))
            subprocess.run(
                ["docker", "cp", tmp_file, "namenode1:/tmp/hdfs_marker_tmp.json"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2
            )
            subprocess.run(
                ["docker", "exec", "namenode1", "hdfs", "dfs", "-put", "-f", "/tmp/hdfs_marker_tmp.json", hdfs_full_path],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2
            )
            return True
        except Exception:
            return False


def get_hdfs_cluster_status():
    """Queries NameNode JMX API (http://localhost:9870/jmx) to retrieve live status
    of the 1 NameNode and 3 DataNodes for the Web Architecture Dashboard.
    """
    status = {
        "namenode_online": False,
        "cluster_name": "bd02-fraud-cluster",
        "namenode_url": HDFS_WEBHDFS_URL,
        "hdfs_uri": "hdfs://namenode1:9000",
        "total_capacity_gb": 0.0,
        "dfs_used_kb": 0.0,
        "total_blocks": 0,
        "total_files": 0,
        "live_datanodes_count": 0,
        "dead_datanodes_count": 0,
        "datanodes": [],
        "hdfs_directories": []
    }
    try:
        jmx_url = f"{HDFS_WEBHDFS_URL}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
        with urllib.request.urlopen(jmx_url, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            beans = data.get("beans", [])
            if beans:
                b = beans[0]
                status["namenode_online"] = True
                status["total_capacity_gb"] = round(b.get("CapacityTotal", 0) / (1024 ** 3), 2)
                status["dfs_used_kb"] = round(b.get("CapacityUsed", 0) / 1024.0, 2)
                status["total_blocks"] = int(b.get("BlocksTotal", 0))
                status["total_files"] = int(b.get("FilesTotal", 0))
                status["live_datanodes_count"] = int(b.get("NumLiveDataNodes", 0))
                status["dead_datanodes_count"] = int(b.get("NumDeadDataNodes", 0))

        # Also query NameNodeInfo for detailed DataNode list
        info_url = f"{HDFS_WEBHDFS_URL}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
        with urllib.request.urlopen(info_url, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            beans = data.get("beans", [])
            if beans:
                live_nodes_raw = json.loads(beans[0].get("LiveNodes", "{}"))
                port_map = {"datanode1": 9864, "datanode2": 9865, "datanode3": 9866}
                idx = 1
                for node_key, node_val in live_nodes_raw.items():
                    node_name = node_key.split(".")[0] if "." in node_key else f"datanode{idx}"
                    status["datanodes"].append({
                        "name": node_name,
                        "Address": node_val.get("infoAddr", ""),
                        "host_port": port_map.get(node_name, 9863 + idx),
                        "state": node_val.get("adminState", "In Service"),
                        "used_kb": round(node_val.get("usedSpace", 0) / 1024.0, 2),
                        "capacity_gb": round(node_val.get("capacity", 0) / (1024 ** 3), 2),
                        "num_blocks": int(node_val.get("numBlocks", 0)),
                        "last_contact_sec": int(node_val.get("lastContact", 0))
                    })
                    idx += 1

        # List files in /bd02_gian_lan via WebHDFS
        list_url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{HDFS_BASE_DIR}?op=LISTSTATUS"
        with urllib.request.urlopen(list_url, timeout=2.0) as resp:
            ldata = json.loads(resp.read().decode("utf-8"))
            statuses = ldata.get("FileStatuses", {}).get("FileStatus", [])
            for st in statuses:
                sub_path = f"{HDFS_BASE_DIR}/{st['pathSuffix']}"
                # Count items inside each directory
                sub_count = 0
                try:
                    sub_url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{sub_path}?op=LISTSTATUS"
                    with urllib.request.urlopen(sub_url, timeout=1.5) as sresp:
                        sdata = json.loads(sresp.read().decode("utf-8"))
                        sub_count = len(sdata.get("FileStatuses", {}).get("FileStatus", []))
                except Exception:
                    pass
                status["hdfs_directories"].append({
                    "path": sub_path,
                    "type": st.get("type", "DIRECTORY"),
                    "items_count": sub_count,
                    "replication": st.get("replication", 3)
                })
    except Exception:
        pass
    return status
