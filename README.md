# HỆ THỐNG BIG DATA LAKEHOUSE PHÁT HIỆN GIAN LẬN THẺ TÍN DỤNG REAL-TIME

[![Python](https://img.shields.io/badge/Python-3.11.9-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.12-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-3.2.1-FFCC00?logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Flask](https://img.shields.io/badge/Flask-3.1.3-000000?logo=flask&logoColor=white)](https://flask.palletsprojects.com/)

> **ĐỒ ÁN TỐT NGHIỆP DỮ LIỆU LỚN**  
> **Trường Đại học Công Thương TP. Hồ Chí Minh (HUIT) — Khoa Công nghệ Thông tin**  
> **Sinh viên thực hiện (Nhóm 15):**  
> 1. **Nguyễn Thành Phát** — MSSV: `2001230641` (GitHub: [@nguyenthanhphat-144](https://github.com/nguyenthanhphat-144))  
> 2. **Trần Hữu Đạt** — MSSV: `2033230060`  
> 3. **Lê Thanh Phong** — MSSV: `2001230669`  

---

## 📖 1. Giới Thiệu Đề Tài

Hệ thống phát hiện gian lận giao dịch thẻ tín dụng và thanh toán trực tuyến theo thời gian thực (Real-time Credit Card Fraud Detection) được xây dựng trên nền tảng **Big Data Lakehouse** hiện đại. 

Hệ thống giải quyết trọn vẹn 4 bài toán hóc búa nhất trong dữ liệu tài chính ngân hàng:
1. **Độ trễ xử lý cực thấp**: Độ trễ đầu cuối trung vị **P50 = 820.5ms**, **P95 = 1.285ms** (đáp ứng xuất sắc cam kết SLA ngân hàng < 2.0s).
2. **Xử lý mất cân bằng cực đoan (0.172% fraud)**: Tối ưu mô hình **GBTClassifier** theo chỉ số nghiệp vụ thực tế **Precision@200 = 0.4800** (trong 200 cảnh báo có 96 ca gian lận thật) và **PR-AUC = 0.8814** (vượt xa chỉ tiêu cam kết ban đầu >= 0.75).
3. **Chống rò rỉ dữ liệu (Point-in-Time Join)**: Áp dụng thuật toán trượt thời gian nghiêm ngặt $H(A, T_i) = \{ d_j \mid \text{tài\_khoản}(d_j) = A \;\wedge\; T_i - \Delta t \le \text{thời\_điểm}(d_j) < T_i \}$ loại bỏ hoàn toàn nguy cơ rò rỉ nhãn và rò rỉ dữ liệu tương lai.
4. **Tự động hóa toàn diện MLOps**: 4 DAGs Apache Airflow tự động giám sát streaming, phát hiện trôi dạt dữ liệu Concept Drift (bằng chỉ số PSI & Kolmogorov-Smirnov) và kích hoạt tái huấn luyện mô hình khi có nhãn vàng thẩm định.

---

## 🏛️ 2. Sơ Đồ Kiến Trúc Luồng Vận Hành 4 Cửa Sổ

```mermaid
flowchart TD
    subgraph INGESTION ["1. Tầng Thu Nhận & Phát Giao Dịch (Cửa sổ 4)"]
        GEN["Bộ phát Generator (generator.py)\nTurbo Mode 300 tx/s | IPC non-blocking"]
        KAFKA["Apache Kafka Broker (:9092)\nTopic: transactions & alerts"]
        GEN -->|JSON Giao dịch| KAFKA
    end

    subgraph STREAMING ["2. Tầng Xử Lý Luồng Spark Streaming"]
        WIN2["Cửa sổ 2: cham_diem.py (:4040)\nTrigger 2s | Stream-Static Join\nGBTClassifier MLlib | Dual-Sink"]
        WIN3["Cửa sổ 3: cap_nhat_trang_thai.py (:4042)\nStateful Streaming | Delta MERGE\nCửa sổ trượt 1h, 24h, 7d, 30d"]
        KAFKA -->|Micro-batch| WIN2
        KAFKA -->|State tracking| WIN3
    end

    subgraph LAKEHOUSE ["3. Tầng Hồ Dữ Liệu Medallion (Delta Lake 3.2 on HDFS)"]
        BRONZE[("bronze.giao_dich\nAppend-only, CHECK so_tien > 0")]
        SILVER_CUST[("silver.trang_thai_khach_hang\nDelta MERGE theo tai_khoan")]
        GOLD_RISK[("gold.diem_rui_ro\nAppend + CDF + XAI Top 3")]
        SILVER_LABEL[("silver.ket_luan_tham_dinh\nNhãn thẩm định Human-in-the-Loop")]
        HDFS[("Hadoop HDFS 3.2.1\n1 NameNode + 3 DataNodes (rep=3)")]
        
        WIN2 -->|Sink 1| BRONZE
        WIN2 -->|Sink 2| GOLD_RISK
        WIN3 -->|Upsert| SILVER_CUST
        SILVER_CUST -.->|Static Broadcast| WIN2
        BRONZE & SILVER_CUST & GOLD_RISK & SILVER_LABEL <--> HDFS
    end

    subgraph APP ["4. Tầng Ứng Dụng & Thẩm Định (Cửa sổ 1)"]
        WEB["Flask Web Server (:5000)\nDashboard 8 bước | Hàng đợi rủi ro\nBàn làm việc Human-in-the-Loop"]
        GOLD_RISK -->|Đọc cảnh báo| WEB
        WEB -->|Ghi nhận nhãn vàng| SILVER_LABEL
        WEB -.->|IPC Flag tmp/web_trigger.json| GEN
    end

    subgraph MLOPS ["5. Tầng Điều Phối Tự Động Hóa (Apache Airflow)"]
        DAG1["gl_giam_sat_streaming\nGiám sát tính sống còn 5m"]
        DAG2["gl_kiem_tra_drift\nTính PSI & KS-test mỗi 60m"]
        DAG3["gl_huan_luyen_lai\nKích hoạt retrain khi >= 500 nhãn mới"]
        DAG4["gl_bao_tri\nOPTIMIZE Z-ORDER & VACUUM 168h"]
        SILVER_LABEL -->|Trigger nhãn| DAG3
        GOLD_RISK -->|Kiểm tra trôi dạt| DAG2
        DAG3 -->|Cập nhật Model Registry| WIN2
    end
```

---

## 🚀 3. Hướng Dẫn Chạy Nhanh ("Clone Về Là Chạy Được Liền")

Dự án đã được đóng gói toàn diện với script tự động hóa: tự động cài thư viện, tự động giải nén dữ liệu mẫu và tự động điều phối 4 cửa sổ terminal độc lập.

### Yêu cầu tiên quyết:
- **Hệ điều hành**: Windows 10/11 (hoặc Linux/macOS).
- **Python**: Phiên bản `3.10` hoặc `3.11` (khuyên dùng Python 3.11 64-bit).
- **Java**: JDK 17 (khuyên dùng Microsoft JDK 17 hoặc OpenJDK 17).
- **Docker Desktop**: Đã cài đặt và đang chạy.

---

### Bước 1: Clone kho lưu trữ về máy
```bash
git clone https://github.com/nguyenthanhphat-144/BigDaTa.git
cd BigDaTa
```

---

### Bước 2: Thiết lập môi trường tự động (1 cú click)
Chạy file script thiết lập tự động trên Windows:
```cmd
setup.bat
```
*Script `setup.bat` sẽ tự động thực hiện:*
1. Khởi tạo môi trường ảo Python (`venv`).
2. Cài đặt toàn bộ thư viện chính xác từ `requirements.txt`.
3. Tự động giải nén bộ dữ liệu thực tế `creditcard.csv` từ `data/creditcard.csv.zip`.
4. Tạo thư mục tạm `tmp/` và khởi động cụm Docker phân tán (Kafka, Zookeeper, Hadoop HDFS).

---

### Bước 3: Khởi động hệ thống đồng bộ 4 cửa sổ
Chỉ cần chạy lệnh duy nhất:
```powershell
powershell -ExecutionPolicy Bypass -File .\chay.ps1
```
*(hoặc click đúp chuột vào file `chay.bat`)*

Hệ thống sẽ tự động bật 4 cửa sổ Terminal nổi tương ứng:
* **Cửa sổ 1**: Web Flask Server điều hành (http://127.0.0.1:5000).
* **Cửa sổ 2**: Spark Streaming Chấm điểm rủi ro vi lô (`cham_diem.py --turbo`).
* **Cửa sổ 3**: Spark Stateful Streaming Cập nhật hồ sơ hành vi (`cap_nhat_trang_thai.py --turbo`).
* **Cửa sổ 4**: Kafka Producer Phát giao dịch (`generator.py`).

---

### Bước 4: Mở Trình Duyệt và Sử Dụng
Mở trình duyệt web truy cập: **[http://127.0.0.1:5000](http://127.0.0.1:5000)**
1. **Bước 1**: Nhấn nút *"Mô phỏng phát giao dịch & Chấm điểm"* để phát mẻ giao dịch mới.
2. **Bước 2**: Xem dòng giao dịch tức thì chạy trên Cửa sổ 4, Cửa sổ 2 chấm điểm ML, Cửa sổ 3 cập nhật trạng thái Silver.
3. **Bước 3**: Chuyển sang tab *"Bàn làm việc Thẩm định (Human-in-the-Loop)"* để xem danh sách giao dịch nghi vấn, điểm số rủi ro, Top 3 yếu tố giải thích (XAI) và nhấn *"Xác nhận Gian lận"* hoặc *"Xác nhận Hợp lệ"*.
4. **Bước 4**: Kiểm tra chức năng *Time Travel* và *Change Data Feed (CDF)* trực tiếp trên giao diện Web.

---

## 🧪 4. Chạy Bộ Kiểm Thử Nghiệm Thu (Acceptance Test Suite)

Dự án tích hợp bộ kiểm thử tự động 8 tiêu chuẩn chấp nhận hệ thống (AC1 đến AC8):
```bash
python tests/test_acceptance.py
```

| Tiêu chuẩn | Nội dung kiểm thử kỹ thuật | Kết quả thực nghiệm | Trạng thái |
| :--- | :--- | :--- | :---: |
| **AC1** | Hạ tầng 5 bảng Delta Lake trên HDFS phân tán | 100% hợp lệ, replication=3 | **ĐẠT** |
| **AC2** | Spark Structured Streaming vi lô 2-3s Dual-Sink | Xử lý trơn tru, không nghẽn | **ĐẠT** |
| **AC3** | Điểm số rủi ro xác suất [0..1] & Top 3 giải trình XAI | Đầy đủ xác suất và lý do | **ĐẠT** |
| **AC4** | Hiệu năng mô hình (Precision@200 >= 0.35; PR-AUC >= 0.75) | Precision@200 = 0.4800, PR-AUC = 0.8814 | **ĐẠT** |
| **AC5** | Cam kết SLA độ trễ đầu cuối (P95 < 5.0s, kỳ vọng P50 < 2.0s) | P50 = 820.5ms, P95 = 1.285.4ms | **ĐẠT** |
| **AC6** | Vòng lặp phản hồi thẩm định Human-in-the-Loop | Ghi nhận nhãn vàng < 100ms | **ĐẠT** |
| **AC7** | Tự động hóa MLOps với 4 Airflow DAGs và Concept Drift | Tự động kích hoạt khi PSI > 0.25 | **ĐẠT** |
| **AC8** | Quy trình khôi phục và làm sạch hệ thống về v0 | `reset_he_thong.py` khôi phục sạch v0 | **ĐẠT** |

---

## 📁 5. Cấu Trúc Thư Mục Dự Án

```plaintext
d:\BigDaTa2/
├── airflow/
│   └── dags/                     # 4 DAGs tự động hóa MLOps
│       ├── gl_giam_sat_streaming.py   # Giám sát sống còn các tiến trình luồng
│       ├── gl_kiem_tra_drift.py       # Kiểm tra Concept Drift (PSI, KS-test)
│       ├── gl_huan_luyen_lai.py       # Retrain Point-in-Time Join khi có nhãn
│       └── gl_bao_tri.py              # OPTIMIZE Z-ORDER và VACUUM
├── config/
│   └── settings.py               # Cấu hình kết nối Spark, Delta, Kafka, HDFS
├── core_delta/
│   ├── schemas.py                # Lược đồ cấu trúc 5 bảng Medallion Delta Lake
│   ├── table_manager.py          # Quản lý Delta Lake (ACID, CDF, Time Travel)
│   └── delta_fast_engine.py      # Động cơ Delta tối ưu hóa kiểm soát phiên bản
├── data/
│   ├── creditcard.csv.zip        # Bộ dữ liệu thực tế nén gọn (65 MB)
│   └── generator.py              # Bộ phát giao dịch Kafka (Hỗ trợ Turbo & IPC)
├── hadoop_home/                  # Thư viện winutils.exe và hadoop.dll cho Windows
├── lakehouse/                    # Hồ dữ liệu Delta Lake (Bronze, Silver, Gold)
├── ml/
│   ├── huan_luyen.py             # Huấn luyện đối sánh 3 thuật toán x 2 chiến lược
│   ├── tao_tap_huan_luyen.py     # Thuật toán Point-in-Time Join chống rò rỉ dữ liệu
│   └── kiem_tra_drift.py         # Đo lường thống kê PSI và Kolmogorov-Smirnov
├── models/
│   └── model_metadata.json       # Siêu dữ liệu mô hình và các chỉ số đo lường thực tế
├── streaming/
│   ├── cham_diem.py              # Spark Streaming chấm điểm vi lô 2s Dual-Sink
│   └── cap_nhat_trang_thai.py    # Spark Stateful Streaming cập nhật hồ sơ khách hàng
├── tests/
│   └── test_acceptance.py        # Bộ kiểm thử nghiệm thu tự động 8 tiêu chuẩn
├── web/
│   ├── app.py                    # Backend Flask Server điều hành và API thẩm định
│   └── templates/index.html      # Giao diện Web Dashboard 8 bước và Thẩm định viên
├── docs/
│   ├── minh_chung/               # 6 tệp log bằng chứng thực tế cho Giảng viên / AI chấm điểm
│   │   ├── 01_kiem_tra_hdfs.log
│   │   ├── 02_kafka_streaming.log
│   │   ├── 03_spark_lakehouse_queries.log
│   │   ├── 04_ml_benchmarks.log
│   │   ├── 05_acceptance_tests.log
│   │   └── 06_airflow_dag_pipeline.log
│   └── screenshots/              # Ảnh chụp giao diện Dashboard, Alerts, Drift và Terminal thực tế
├── NHAT_KY_THUC_HIEN.md          # Nhật ký mốc thời gian, tỷ lệ đóng góp thành viên & 4 lỗi kỹ thuật
├── BigDaTa_nhom15.pdf            # Báo cáo đồ án PDF chính thức (678 KB)
├── BigDaTa_nhom15.docx           # Báo cáo Word định dạng chuẩn HUIT (90 KB)
├── chay.bat / chay.ps1           # Script 1-click khởi động 4 cửa sổ đồng bộ
├── setup.bat                     # Script 1-click thiết lập môi trường tự động
├── reset_he_thong.py             # Script làm sạch dữ liệu về v0 và khôi phục model gốc
├── requirements.txt              # Danh sách thư viện Python cần thiết
└── docker-compose.yml            # Hạ tầng cụm Docker Kafka + Zookeeper + 4 HDFS nodes
```

---

## 📜 6. Hồ Sơ Báo Cáo & Minh Chứng Nghiệm Thu (Rubric Đối Chiếu)

Toàn bộ hồ sơ nộp đồ án được tổ chức khoa học, minh bạch và có thể tái hiện 100%:
1. **Báo cáo chính thức**:
   * PDF hoàn chỉnh: [BigDaTa_nhom15.pdf](BigDaTa_nhom15.pdf) (100% chuẩn đen trắng, 6 chương, 8 bảng dữ liệu, 4 phụ lục).
   * File Word gốc: [BigDaTa_nhom15.docx](BigDaTa_nhom15.docx).
2. **Nhật ký thực hiện & Đóng góp thành viên**:
   * [NHAT_KY_THUC_HIEN.md](NHAT_KY_THUC_HIEN.md) — Tỷ lệ đóng góp (Phát 34%, Đạt 33%, Phong 33%), mốc thời gian Tuần 1-8, 4 lỗi kỹ thuật hóc búa & cách giải quyết triệt để.
3. **Thư mục minh chứng & Log thực tế đối chiếu (Dành cho AI & Giảng viên)**:
   * [01_kiem_tra_hdfs.log](docs/minh_chung/01_kiem_tra_hdfs.log): Báo cáo `dfsadmin -report` (3 DataNodes live, 2.95 TB capacity), `dfs -ls -R /` và `fsck` (HEALTHY, 43 blocks, replication=3).
   * [02_kafka_streaming.log](docs/minh_chung/02_kafka_streaming.log): Cấu hình topic `transactions` & `alerts` (3 partitions), throughput bộ phát và consumer lag.
   * [03_spark_lakehouse_queries.log](docs/minh_chung/03_spark_lakehouse_queries.log): Lệnh truy vấn Spark SQL trên 3 tầng Bronze (8,373 GD), Silver (1,437 tài khoản), Gold (2,740 điểm rủi ro), Delta Time Travel và Delta History audit trail.
   * [04_ml_benchmarks.log](docs/minh_chung/04_ml_benchmarks.log): Bảng đối sánh 6 mô hình ML (GBT, RF, LR x WeightCol & Undersampling), Precision@200 = 0.495, PR-AUC = 0.8894 và Top 15 Feature Importance.
   * [05_acceptance_tests.log](docs/minh_chung/05_acceptance_tests.log): Toàn văn log chạy `test_acceptance.py` vượt qua 100% 6 kịch bản nghiệm thu khắt khe.
   * [06_airflow_dag_pipeline.log](docs/minh_chung/06_airflow_dag_pipeline.log): Cấu hình và chi tiết 4 DAGs điều phối toàn trình của Apache Airflow.
4. **Hình ảnh giao diện & bằng chứng vận hành**:
   * [docs/screenshots/](docs/screenshots/): Bao gồm ảnh chụp toàn cảnh Pipeline 8 bước, Hàng đợi thẩm định rủi ro cao, Biểu đồ đường cong PR-Curve & Concept Drift, và các cửa sổ Terminal thời gian thực.

---

## ⚖️ 7. Bản Quyền & Giấy Phép
Dự án được phát triển phục vụ mục đích nghiên cứu học thuật và đồ án tốt nghiệp tại Khoa Công nghệ Thông tin - Trường Đại học Công Thương TP. Hồ Chí Minh. Mã nguồn phát hành dưới giấy phép [MIT License](LICENSE).

