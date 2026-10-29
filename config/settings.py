import os
import sys
import json
import time
import socket
import datetime
import subprocess
import urllib.request
from urllib.parse import quote

IS_DOCKER = os.path.exists("/.dockerenv")

# Automatically resolve Docker container hostnames ('kafka', 'namenode1', 'datanode1..3') to 127.0.0.1 when running on Windows Host
if sys.platform == "win32" and not IS_DOCKER:
    _orig_getaddrinfo = socket.getaddrinfo

    def _patched_getaddrinfo(host, port, *args, **kwargs):
        h_str = str(host or "")
        if h_str in ("kafka", "namenode1", "datanode1", "datanode2", "datanode3") or h_str.startswith(("namenode1.", "datanode1.", "datanode2.", "datanode3.", "kafka.")):
            host = "127.0.0.1"
        return _orig_getaddrinfo(host, port, *args, **kwargs)

    socket.getaddrinfo = _patched_getaddrinfo

# Base project directories
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
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_INTERNAL_SERVERS",
    "kafka:9092" if IS_DOCKER else "localhost:9092"
)
KAFKA_TOPIC_TRANSACTIONS = "transactions"
KAFKA_TOPIC_ALERTS = "alerts"
KAFKA_CONSUMER_GROUP = "bd02_fraud_scoring_group"
KAFKA_STATE_CONSUMER_GROUP = "bd02_customer_state_group"

# HDFS Configuration (1 NameNode + 3 DataNodes, replication=3)
HDFS_NAMENODE_URI = "hdfs://namenode1:9000"
HDFS_WEBHDFS_URL = os.environ.get(
    "HDFS_WEBHDFS_INTERNAL",
    "http://namenode1:9870" if IS_DOCKER else "http://localhost:9870"
)
HDFS_BASE_DIR = "/lakehouse"
HDFS_RAW_DIR = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/raw"
HDFS_MODELS_DIR = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/models"
HDFS_CREDITCARD_CSV = f"{HDFS_RAW_DIR}/creditcard.csv"

# Real HDFS Delta Lake Medallion Table Paths (Stored directly on HDFS DataNodes with replication=3)
BRONZE_GIAO_DICH = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/bronze/giao_dich"
SILVER_TRANG_THAI = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/silver/trang_thai_khach_hang"
SILVER_KET_LUAN = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/silver/ket_luan_tham_dinh"
SILVER_TAP_HUAN_LUYEN = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/silver/tap_huan_luyen"
GOLD_DIEM_RUI_RO = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/gold/diem_rui_ro"
GOLD_THEO_DOI_DRIFT = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/gold/theo_doi_drift"
QUARANTINE_DIR = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/_quarantine"

# Structured Streaming Checkpoint Paths (Stored on HDFS for distributed fault tolerance)
CHECKPOINT_SCORING = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/_checkpoints/cham_diem"
CHECKPOINT_STATE = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/_checkpoints/cap_nhat_trang_thai"
CHECKPOINT_BRONZE = f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/_checkpoints/bronze_sink"

# Local Source Dataset in d:\BigDaTa2\creditcard.csv (uploaded to HDFS_CREDITCARD_CSV)
CREDITCARD_CSV = os.path.join(BASE_DIR, "creditcard.csv")

# WebHDFS DataNode HTTP port mapping on Windows Host
DATANODE_HTTP_PORT_MAP = {
    "datanode1": 9864,
    "datanode2": 9865,
    "datanode3": 9868,
}

# Ensure required local metadata/temp directories exist
for d in [LAKEHOUSE_DIR, MODELS_DIR, CHECKPOINTS_DIR, TMP_DIR]:
    os.makedirs(d, exist_ok=True)


def ensure_utf8_console():
    """Ensures Windows Console code page is UTF-8 (65001) and sys.std* streams handle UTF-8 cleanly."""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
        except Exception:
            pass
    if hasattr(sys.stderr, "reconfigure"):
        try:
            sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
        except Exception:
            pass
    if sys.platform == "win32" and not IS_DOCKER:
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleOutputCP(65001)
            kernel32.SetConsoleCP(65001)
        except Exception:
            pass


def ensure_hosts_file():
    """Dynamically ensures local machine hostname and Docker container hosts resolve in hosts file on Windows."""
    if IS_DOCKER or sys.platform != "win32":
        return
    try:
        hosts_path = os.path.join(BASE_DIR, "hosts")
        local_host = socket.gethostname()
        required_hosts = [
            ("127.0.0.1", "localhost"),
            ("127.0.0.1", "127.0.0.1"),
            ("127.0.0.1", local_host),
            ("127.0.0.1", "kafka"),
            ("127.0.0.1", "namenode1"),
            ("127.0.0.1", "datanode1"),
            ("127.0.0.1", "datanode2"),
            ("127.0.0.1", "datanode3"),
            ("127.0.0.1", "datanode1.bigdata2_default"),
            ("127.0.0.1", "datanode2.bigdata2_default"),
            ("127.0.0.1", "datanode3.bigdata2_default"),
        ]
        existing_lines = []
        if os.path.exists(hosts_path):
            with open(hosts_path, "r", encoding="utf-8") as f:
                existing_lines = [l.strip() for l in f if l.strip()]
        seen = set()
        new_lines = []
        for line in existing_lines:
            parts = line.split()
            if len(parts) >= 2:
                seen.add(parts[1])
                new_lines.append(line)
        for ip, host in required_hosts:
            if host not in seen:
                seen.add(host)
                new_lines.append(f"{ip} {host}")
        with open(hosts_path, "w", encoding="utf-8") as f:
            f.write("\n".join(new_lines) + "\n")
    except Exception:
        pass


ensure_utf8_console()
ensure_hosts_file()


def set_console_title(title_text):
    """Sets the Windows PowerShell / CMD window title bar to show the currently running file."""
    try:
        if os.name == "nt":
            import ctypes
            ctypes.windll.kernel32.SetConsoleTitleW(str(title_text))
        else:
            sys.stdout.write(f"\x1b]0;{title_text}\x07")
            sys.stdout.flush()
    except Exception:
        pass


def setup_spark_env():
    """Configures environment variables required for PySpark + Delta Lake + HDFS on Windows."""
    os.environ["TEMP"] = TMP_DIR
    os.environ["TMP"] = TMP_DIR
    if sys.platform == "win32" and not IS_DOCKER:
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
    """Creates a SparkSession configured with direct HDFS connection (hdfs://namenode1:9000),
    Delta Lake 3.2, Kafka SQL 0-10, replication=3, and CDF enabled.
    """
    setup_spark_env()
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    target_py = VENV_PYTHON if (sys.platform == "win32" and os.path.exists(VENV_PYTHON)) else sys.executable
    target_py_clean = target_py.replace("\\", "/")

    hosts_opt = f"-Djdk.net.hosts.file={HOSTS_FILE} " if (sys.platform == "win32" and not IS_DOCKER) else ""
    java_17_opts = (
        f"{hosts_opt}"
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
        # Direct HDFS Storage & DataNode Hostname Resolution (1 NameNode + 3 DataNodes, rep=3)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE_URI)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "3")
        # Delta Lake 3.2 ACID & Change Data Feed
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.streaming.checkpointLocation", f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/_checkpoints/default")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.warehouse.dir", f"{HDFS_NAMENODE_URI}{HDFS_BASE_DIR}/warehouse")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "10s")
        .config("spark.port.maxRetries", "64")
        .config("spark.ui.enabled", "true")
        .config("spark.driver.extraJavaOptions", java_17_opts)
        .config("spark.executor.extraJavaOptions", java_17_opts)
    )

    if ui_port:
        builder = builder.config("spark.ui.port", str(ui_port))

    spark = configure_spark_with_delta_pip(builder, [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Bypass Windows NativeIO POSIX permission bug on local temp/shuffle dirs
    if sys.platform.startswith("win"):
        try:
            jvm = spark.sparkContext._jvm
            win_io_class = jvm.Class.forName("org.apache.hadoop.io.nativeio.NativeIO$Windows")
            field = win_io_class.getDeclaredField("nativeLoaded")
            field.setAccessible(True)
            field.setBoolean(None, False)
        except Exception:
            pass

    return spark


# ==============================================================================
# WEBHDFS HELPERS FOR ULTRA-FAST PYTHON / FLASK / AIRFLOW ACCESS TO HDFS
# ==============================================================================
def to_hdfs_rel_path(uri_or_path):
    """Converts 'hdfs://namenode1:9000/lakehouse/...' to '/lakehouse/...'."""
    s = str(uri_or_path).replace("\\", "/")
    if s.startswith("hdfs://"):
        idx = s.find("/", 7)
        return s[idx:] if idx != -1 else "/"
    return s if s.startswith("/") else f"/{s}"


def _rewrite_datanode_url(url_str):
    """Rewrites DataNode container WebHDFS URLs to mapped localhost ports when on Windows Host."""
    if IS_DOCKER:
        return url_str
    for dn_name, host_port in DATANODE_HTTP_PORT_MAP.items():
        if f"{dn_name}:9864" in url_str:
            return url_str.replace(f"{dn_name}:9864", f"localhost:{host_port}")
    return url_str


def hdfs_list_status(uri_or_path):
    """Lists files/directories at an HDFS path via WebHDFS REST API."""
    hdfs_path = quote(to_hdfs_rel_path(uri_or_path), safe="/=_-.")
    url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{hdfs_path}?op=LISTSTATUS&user.name=root"
    try:
        with urllib.request.urlopen(url, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("FileStatuses", {}).get("FileStatus", [])
    except Exception:
        return []


def hdfs_read_bytes(uri_or_path):
    """Reads raw file bytes from HDFS via WebHDFS OPEN."""
    hdfs_path = quote(to_hdfs_rel_path(uri_or_path), safe="/=_-.")
    init_url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{hdfs_path}?op=OPEN&noredirect=true&user.name=root"
    with urllib.request.urlopen(init_url, timeout=2.5) as resp:
        loc_info = json.loads(resp.read().decode("utf-8"))
        dn_url = _rewrite_datanode_url(loc_info.get("Location", ""))
    with urllib.request.urlopen(dn_url, timeout=8.0) as dn_resp:
        return dn_resp.read()


def hdfs_write_bytes(uri_or_path, content_bytes, replication=3):
    """Writes raw file bytes directly to HDFS with replication=3 via WebHDFS CREATE."""
    hdfs_path = quote(to_hdfs_rel_path(uri_or_path), safe="/=_-.")
    init_url = (
        f"{HDFS_WEBHDFS_URL}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&overwrite=true&replication={int(replication)}&noredirect=true&user.name=root"
    )
    req = urllib.request.Request(init_url, method="PUT")
    with urllib.request.urlopen(req, timeout=2.5) as resp:
        loc_info = json.loads(resp.read().decode("utf-8"))
        dn_url = _rewrite_datanode_url(loc_info.get("Location", ""))
    req_put = urllib.request.Request(dn_url, data=content_bytes, method="PUT")
    req_put.add_header("Content-Type", "application/octet-stream")
    with urllib.request.urlopen(req_put, timeout=10.0) as _:
        return True


def ensure_creditcard_on_hdfs():
    """Ensures raw dataset creditcard.csv (284,807 tx) is uploaded to HDFS /lakehouse/raw/creditcard.csv (rep=3)."""
    statuses = hdfs_list_status(HDFS_RAW_DIR)
    for st in statuses:
        if st.get("pathSuffix") == "creditcard.csv" and int(st.get("length", 0)) > 100_000_000:
            return HDFS_CREDITCARD_CSV

    if os.path.exists(CREDITCARD_CSV):
        print(f"[HDFS Ingestion] Dang day creditcard.csv (~143 MB) len {HDFS_CREDITCARD_CSV} (replication=3)...")
        try:
            subprocess.run(
                ["docker", "exec", "namenode1", "hdfs", "dfs", "-mkdir", "-p", f"{HDFS_BASE_DIR}/raw"],
                check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10
            )
            subprocess.run(
                ["docker", "cp", CREDITCARD_CSV, "namenode1:/tmp/creditcard.csv"],
                check=True, timeout=60
            )
            subprocess.run(
                [
                    "docker", "exec", "namenode1", "hdfs", "dfs",
                    "-D", "dfs.replication=3",
                    "-put", "-f", "/tmp/creditcard.csv", f"{HDFS_BASE_DIR}/raw/creditcard.csv"
                ],
                check=True, timeout=90
            )
            subprocess.run(
                ["docker", "exec", "namenode1", "rm", "-f", "/tmp/creditcard.csv"],
                check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10
            )
            print(f"[HDFS Ingestion] Da nap thanh cong {HDFS_CREDITCARD_CSV} tren 3 DataNodes!")
        except Exception as e:
            print(f"[HDFS Ingestion Warning] {e}")
    return HDFS_CREDITCARD_CSV


def log_pipeline_event(source_node, target_node, action, details="", count=0, status="SUCCESS"):
    """Logs a real-time data flow transition between two system architecture nodes."""
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
    events = events[:120]
    try:
        with open(PIPELINE_EVENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2, ensure_ascii=False, default=str)
    except Exception:
        pass
    return event


def write_hdfs_marker(hdfs_subpath, payload_dict):
    """Writes a JSON audit record into /lakehouse/_audit/... on HDFS (replication=3)."""
    content_bytes = json.dumps(payload_dict, indent=2, ensure_ascii=False, default=str).encode("utf-8")
    hdfs_full_path = f"{HDFS_BASE_DIR}/_audit/{hdfs_subpath.lstrip('/')}"
    try:
        return hdfs_write_bytes(hdfs_full_path, content_bytes, replication=3)
    except Exception:
        return False


def get_hdfs_cluster_status():
    """Queries NameNode JMX API (http://localhost:9870/jmx) to retrieve live status
    of the 1 NameNode and 3 DataNodes + real Medallion directories in /lakehouse.
    """
    status = {
        "namenode_online": False,
        "cluster_name": "bd02-fraud-cluster",
        "namenode_url": "http://localhost:9870",
        "hdfs_uri": HDFS_NAMENODE_URI,
        "hdfs_lakehouse_root": HDFS_BASE_DIR,
        "total_capacity_gb": 0.0,
        "dfs_used_kb": 0.0,
        "dfs_used_mb": 0.0,
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
                used_bytes = b.get("CapacityUsed", 0)
                status["total_capacity_gb"] = round(b.get("CapacityTotal", 0) / (1024 ** 3), 2)
                status["dfs_used_kb"] = round(used_bytes / 1024.0, 2)
                status["dfs_used_mb"] = round(used_bytes / (1024 ** 2), 2)
                status["total_blocks"] = int(b.get("BlocksTotal", 0))
                status["total_files"] = int(b.get("FilesTotal", 0))
                status["live_datanodes_count"] = int(b.get("NumLiveDataNodes", 0))
                status["dead_datanodes_count"] = int(b.get("NumDeadDataNodes", 0))

        info_url = f"{HDFS_WEBHDFS_URL}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
        with urllib.request.urlopen(info_url, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            beans = data.get("beans", [])
            if beans:
                live_nodes_raw = json.loads(beans[0].get("LiveNodes", "{}"))
                idx = 1
                for node_key, node_val in sorted(live_nodes_raw.items()):
                    node_name = node_key.split(":")[0].split(".")[0] if (":" in node_key or "." in node_key) else f"datanode{idx}"
                    status["datanodes"].append({
                        "name": node_name,
                        "Address": node_val.get("xferAddr", node_val.get("infoAddr", "")),
                        "host_port": DATANODE_HTTP_PORT_MAP.get(node_name, 9863 + idx),
                        "state": node_val.get("adminState", "In Service"),
                        "used_kb": round(node_val.get("usedSpace", 0) / 1024.0, 2),
                        "used_mb": round(node_val.get("usedSpace", 0) / (1024 ** 2), 2),
                        "capacity_gb": round(node_val.get("capacity", 0) / (1024 ** 3), 2),
                        "num_blocks": int(node_val.get("numBlocks", 0)),
                        "last_contact_sec": int(node_val.get("lastContact", 0))
                    })
                    idx += 1

        statuses = hdfs_list_status(HDFS_BASE_DIR)
        for st in statuses:
            sub_path = f"{HDFS_BASE_DIR}/{st['pathSuffix']}"
            sub_items = hdfs_list_status(sub_path)
            status["hdfs_directories"].append({
                "path": sub_path,
                "type": st.get("type", "DIRECTORY"),
                "items_count": len(sub_items) if sub_items else (1 if st.get("type") == "FILE" else 0),
                "replication": st.get("replication", 3) or 3
            })
    except Exception:
        pass
    return status
