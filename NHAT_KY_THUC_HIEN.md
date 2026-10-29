# NHẬT KÝ THỰC HIỆN ĐỒ ÁN BIG DATA
## ĐỀ TÀI: HỆ THỐNG PHÁT HIỆN GIAN LẬN THẺ TÍN DỤNG THỜI GIAN THỰC TRÊN NỀN TẢNG BIG DATA LAKEHOUSE (HADOOP HDFS, KAFKA, SPARK STREAMING, DELTA LAKE VÀ APACHE AIRFLOW)

---

### PHẦN 1: BẢNG ĐÓNG GÓP VÀ PHÂN CÔNG THÀNH VIÊN (NHÓM 15)

| STT | Họ và Tên | MSSV | Vai trò | Nhiệm vụ kỹ thuật trực tiếp đảm nhận & Tệp mã nguồn phụ trách | Tỷ lệ đóng góp |
| :---: | :--- | :---: | :---: | :--- | :---: |
| 1 | **Nguyễn Thành Phát** | **2001230641** | **Nhóm trưởng** | - Thiết kế kiến trúc 5 tầng End-to-End và cấu hình cụm 7 container trong `docker-compose.yml` (1 NameNode + 3 DataNodes cổng riêng `9866/9876/9886`, Kafka, Zookeeper, Airflow).<br>- Xây dựng kết nối trực tiếp Spark $\leftrightarrow$ HDFS (`hdfs://namenode1:9000/lakehouse`, `replication=3`) trong `config/settings.py` và `core_delta/table_manager.py`.<br>- Hiện thực hóa luồng Spark Structured Streaming `streaming/cham_diem.py` (Dual-Sink, Watermark 1m, Stream-Static Join) và `streaming/cap_nhat_trang_thai.py` (Stateful Delta `MERGE INTO`).<br>- Xây dựng script vận hành `bootstrap_pipeline.py`, `chay.ps1`, `reset_he_thong.py`. | **34%** |
| 2 | **Trần Hữu Đạt** | **2033230060** | **Thành viên** | - Phân tích khám phá dữ liệu (EDA) trên tập ULB `creditcard.csv` (`284,807` giao dịch, `492` gian lận `0.1727%`) lưu trên `hdfs://namenode1:9000/lakehouse/raw/creditcard.csv`.<br>- Hiện thực hóa thuật toán **Point-in-Time Join** chống rò rỉ dữ liệu thời gian trên khoảng `[T - Window, T - 1s]` tại `ml/tao_tap_huan_luyen.py`.<br>- Lập trình `ml/huan_luyen.py` huấn luyện và đối sánh 6 mô hình (3 thuật toán `GBTClassifier`, `RandomForestClassifier`, `LogisticRegression` $\times$ 2 chiến lược `WeightCol` & `Undersampling 1:20`), đóng gói `PipelineModel` lên HDFS (`Precision@200 = 0.8600`, `PR-AUC = 0.9420`).<br>- Xây dựng phân hệ làm sạch & cách ly bản ghi lỗi `transform/bronze_to_silver.py`. | **33%** |
| 3 | **Lê Thanh Phong** | **2001230669** | **Thành viên** | - Thiết kế và vận hành thật 4 DAG Apache Airflow (`:8080`) trong `airflow/dags/`: `gl_giam_sat_streaming.py` (`*/5 * * * *`), `gl_kiem_tra_drift.py` (`0 * * * *`), `gl_huan_luyen_lai.py` (`0 3 * * 0`), `gl_bao_tri.py` (`0 2 * * 0`).<br>- Xây dựng module đo lường Concept Drift bằng PSI & kiểm định Kolmogorov-Smirnov tại `ml/kiem_tra_drift.py`.<br>- Phát triển ứng dụng Web Flask (`web/app.py`, `web/templates/index.html`) tích hợp `DeltaFastEngine` đọc/ghi trực tiếp HDFS qua WebHDFS và bàn thẩm định Human-in-the-Loop.<br>- Xây dựng bộ phát giao dịch `data/generator.py` và bộ kiểm thử nghiệm thu `tests/test_acceptance.py` (Pass 100%). | **33%** |
| **TỔNG** | | | | | **100%** |

---

### PHẦN 2: CÁC MỐC THỜI GIAN THỰC HIỆN DỰ ÁN (MILESTONES)

| Mốc thời gian | Giai đoạn | Nội dung công việc chi tiết | Đầu ra / Bằng chứng đối chiếu |
| :--- | :--- | :--- | :--- |
| **Tuần 1 - 2** | Khảo sát & Hạ tầng Cụm Phân tán | - Phân tích yêu cầu độ trễ `< 2s` và bài toán mất cân bằng cực đoan `0.1727%` trên tập dữ liệu ULB (`284,807` dòng).<br>- Thiết lập `docker-compose.yml` gồm 1 NameNode (`:9870`, `:9000`), 3 DataNodes (`datanode1..3`), Zookeeper (`:2181`), Kafka (`:9092`) và Apache Airflow (`:8080`).<br>- Nạp `creditcard.csv` lên `hdfs://namenode1:9000/lakehouse/raw/creditcard.csv` với `replication=3`. | - `docker-compose.yml`<br>- `docs/minh_chung/01_kiem_tra_hdfs.log`<br>- Ảnh `07_hdfs_namenode_ui.png` |
| **Tuần 3 - 4** | Ingestion & Spark Structured Streaming | - Lập trình `data/generator.py` phát giao dịch vào Kafka topic `transactions` (3 partitions, Turbo 300 tx/s, 15–18% out-of-order $\le 30\text{s}$).<br>- Hiện thực hóa `streaming/cham_diem.py` (Spark UI `:4040`) và `streaming/cap_nhat_trang_thai.py` (Spark UI `:4042`) đọc Kafka và ghi trực tiếp bảng Delta lên HDFS `/lakehouse`. | - `data/generator.py`, `streaming/cham_diem.py`<br>- `docs/minh_chung/02_kafka_streaming.log`<br>- Ảnh `08_spark_web_ui_streaming.png` |
| **Tuần 5** | Medallion Lakehouse & Point-in-Time Join | - Thiết lập 5 bảng Medallion (`bronze.giao_dich`, `silver.trang_thai_khach_hang`, `silver.ket_luan_tham_dinh`, `silver.tap_huan_luyen`, `gold.diem_rui_ro`, `gold.theo_doi_drift`) + `_quarantine` trên HDFS.<br>- Áp dụng ràng buộc `CHECK (so_tien > 0)`, Change Data Feed (CDF) và Time Travel.<br>- Lập trình `ml/tao_tap_huan_luyen.py` tính đặc trưng cửa sổ trượt nghiêm ngặt `[T - Window, T - 1s]`. | - `core_delta/table_manager.py`<br>- `ml/tao_tap_huan_luyen.py`<br>- `docs/minh_chung/03_spark_lakehouse_queries.log` |
| **Tuần 6** | Huấn luyện & Đối sánh 6 Mô hình Học máy | - Chia dữ liệu Chronological Split (70% Train, 15% Validation, 15% Test) từ bảng HDFS `silver.tap_huan_luyen`.<br>- Đối sánh 3 thuật toán (`GBTClassifier`, `RandomForestClassifier`, `LogisticRegression`) $\times$ 2 chiến lược (`WeightCol`, `Undersampling 1:20`).<br>- Lưu `PipelineModel` chiến thắng (`Precision@200 = 0.8600`, `PR-AUC = 0.9420`, `ROC-AUC = 0.9880`) trực tiếp lên `hdfs://namenode1:9000/lakehouse/models/model_v1`. | - `ml/huan_luyen.py`<br>- `models/model_metadata.json`<br>- `docs/minh_chung/04_ml_benchmarks.log` |
| **Tuần 7** | Web Dashboard & Human-in-the-Loop | - Xây dựng `web/app.py` với `DeltaFastEngine` truy xuất trực tiếp Parquet/Delta từ HDFS qua WebHDFS.<br>- Hiển thị hàng đợi cảnh báo rủi ro cao kèm Top 3 yếu tố giải trình XAI và ghi nhãn chuyên gia vào `silver.ket_luan_tham_dinh` trên HDFS. | - `web/app.py`, `web/templates/index.html`<br>- Ảnh `01_pipeline_tong_quan.png`, `02_hang_doi_canh_bao_va_tham_dinh.png`, `03_bieu_do_pr_curve_va_drift.png` |
| **Tuần 8** | Điều phối Airflow Docker & Nghiệm thu | - Triển khai và kích hoạt chạy thật 4 DAG trong container Airflow (`:8080`) đạt trạng thái `SUCCESS`.<br>- Kiểm chứng `OPTIMIZE ZORDER BY` (giảm thời gian truy vấn từ `1032.49ms` xuống `433.65ms`, nhanh hơn `58.0%`).<br>- Chạy bộ kiểm thử tự động `tests/test_acceptance.py` đạt 100% và đóng gói hồ sơ nộp. | - `airflow/dags/*.py`<br>- `docs/minh_chung/05_acceptance_tests.log` & `06_airflow_dag_pipeline.log`<br>- Ảnh `09_airflow_web_ui_dags.png` |

---

### PHẦN 3: CÁC LỖI KỸ THUẬT THỰC TẾ ĐÃ GẶP VÀ GIẢI PHÁP TRIỆT ĐỂ

#### 1. Lỗi Định tuyến Khối Dữ liệu Datanode Nội bộ Docker khi Spark trên Windows Ghi Trực tiếp lên Cụm 3 DataNodes HDFS
* **Hiện tượng**: Khi Spark trên Windows kết nối `hdfs://namenode1:9000` để ghi bảng Delta với `dfs.replication=3`, NameNode trả về danh sách IP nội bộ mạng bridge Docker (`172.19.0.x:9866`) khiến DFSClient trên Windows bị treo kết nối (`ConnectTimeoutException`) hoặc xung đột cổng `9866` giữa 3 DataNodes.
* **Giải pháp triệt để**:
  1. Bật `dfs.client.use.datanode.hostname=true` và `dfs.datanode.use.datanode.hostname=true` trên cả cụm HDFS và SparkSession.
  2. Cấu hình cổng truyền block (`dfs.datanode.address`) riêng biệt cho từng DataNode trong `docker-compose.yml`: `datanode1` (`9866`), `datanode2` (`9876`), `datanode3` (`9886`), đồng thời giữ cổng HTTP nội bộ `:9864` (ánh xạ ra host `9864`, `9865`, `9868`) để healthcheck luôn `healthy`.
  3. Ánh xạ tên miền `namenode1`, `datanode1..3` về `127.0.0.1` qua `-Djdk.net.hosts.file=d:/BigDaTa2/hosts`, giúp Spark ghi trực tiếp mọi block Parquet/Delta đồng thời lên cả 3 DataNodes (`replication=3`).

#### 2. Lỗi Xung đột Broker ID của Kafka với Metadata Zookeeper khi Khởi động lại Container (`Leader: none`)
* **Hiện tượng**: Khi khởi động lại container `wurstmeister/kafka` mà không cố định `KAFKA_BROKER_ID`, Kafka tự sinh ID mới (`1003`) trong khi Zookeeper vẫn giữ phân vùng `transactions-0` thuộc về broker cũ (`1001`), dẫn tới `Leader: none` và lỗi `TimeoutException: Timeout of 60000ms expired before the position for partition transactions-0 could be determined`.
* **Giải pháp triệt để**: Cố định `KAFKA_BROKER_ID: 1` trong `docker-compose.yml` và khởi tạo đồng bộ 3 phân vùng (`PartitionCount: 3`, `Leader: 1`, `Isr: 1`) cho cả hai topic `transactions` và `alerts`.

#### 3. Lỗi Windows Winutils, NativeIO POSIX Permission & Xung đột URI Scheme khi `fs.defaultFS` trỏ lên HDFS
* **Hiện tượng**: Trên Windows, PySpark ném lỗi `UnsatisfiedLinkError: NativeIO$Windows.access0` khi ghi shuffle tạm, và khi đặt `spark.hadoop.fs.defaultFS = hdfs://namenode1:9000`, các đường dẫn cục bộ dạng `d:\BigDaTa2\models\model_v1` bị hiểu nhầm thành đường dẫn tương đối trên HDFS (`Pathname /d:/BigDaTa2/... is not a valid DFS filename`).
* **Giải pháp triệt để**: Tích hợp `hadoop_home/bin/winutils.exe` + `hadoop.dll`, vô hiệu hóa cờ `NativeIO$Windows.nativeLoaded` qua JVM reflection trong `config/settings.py`, lưu `PipelineModel` trực tiếp lên `hdfs://namenode1:9000/lakehouse/models/model_v1` và chuẩn hóa bản sao lưu cục bộ bằng tiền tố tường minh `file:///d:/BigDaTa2/...`.

#### 4. Lỗi Rò rỉ Dữ liệu Thời gian (Data Leakage) khi Tính Đặc trưng Hành vi Cửa sổ Trượt
* **Hiện tượng**: Nếu sử dụng `rangeBetween(-3600, 0)` trên cột thời gian, bản thân giao dịch đang xét tại thời điểm $T_i$ bị tính gộp vào tổng tiền và số lần giao dịch lịch sử, làm sai lệch phân phối giữa lúc huấn luyện và lúc chấm điểm trực tuyến.
* **Giải pháp triệt để**: Áp dụng khoảng trượt nghiêm ngặt `.rangeBetween(-w_sec, -1)` tại `ml/tao_tap_huan_luyen.py` (chỉ lấy $[T_i - \Delta t, T_i - 1\text{s}]$) và kiểm chứng tự động bằng unit test `test_02_point_in_time_join_no_data_leakage`.

#### 5. Lỗi Phình to State Store do Bản tin Đến trễ (Out-of-Order Events) & JVM Java 17 Module Reflection
* **Hiện tượng**: Luồng giao dịch thực tế có 15–18% gói tin đến trễ tối đa 30s và chạy song song nhiều tiến trình Spark trên Java 17 gặp lỗi chặn truy cập `sun.nio.ch` / trùng cổng Spark UI `4040`.
* **Giải pháp triệt để**: Đặt `.withWatermark("thoi_diem", "1 minute")`, tách cổng Spark UI (`4040` cho `cham_diem.py`, `4042` cho `cap_nhat_trang_thai.py`), bổ sung đầy đủ cờ `--add-opens=java.base/java.nio=ALL-UNNAMED` và dọn dẹp tài khoản không hoạt động quá 30 ngày trong DAG `gl_bao_tri`.
