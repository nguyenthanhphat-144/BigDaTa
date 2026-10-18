from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    LongType, TimestampType, DateType, ArrayType
)

# 2.4.1. Bảng bronze.giao_dich (Lưu trữ thô, ghi nguyên trạng, phân vùng theo ngay)
bronze_fields = [
    StructField("ma_giao_dich", StringType(), False),
    StructField("thoi_diem", TimestampType(), False),
    StructField("thoi_diem_nhan", TimestampType(), False),
    StructField("tai_khoan_nguon", StringType(), False),
    StructField("tai_khoan_dich", StringType(), False),
    StructField("loai", StringType(), False),  # CHUYEN_KHOAN | RUT_TIEN | THANH_TOAN | NAP_TIEN
    StructField("so_tien", DoubleType(), False),
    StructField("so_du_truoc_nguon", DoubleType(), True),
    StructField("so_du_sau_nguon", DoubleType(), True),
    StructField("so_du_truoc_dich", DoubleType(), True),
    StructField("so_du_sau_dich", DoubleType(), True),
    StructField("kenh", StringType(), True),  # ATM | POS | ONLINE | APP
    StructField("ngay", DateType(), True),
    # Schema Evolution fields (Tuần 5: ma_thiet_bi, dia_chi_ip)
    StructField("ma_thiet_bi", StringType(), True),
    StructField("dia_chi_ip", StringType(), True),
] + [StructField(f"v{i}", DoubleType(), True) for i in range(1, 29)]

BRONZE_GIAO_DICH_SCHEMA = StructType(bronze_fields)

# Kafka incoming raw message schema (JSON deserialization from topic 'transactions')
kafka_fields = [
    StructField("ma_giao_dich", StringType(), False),
    StructField("thoi_diem", StringType(), False),
    StructField("thoi_diem_nhan", StringType(), True),
    StructField("tai_khoan_nguon", StringType(), False),
    StructField("tai_khoan_dich", StringType(), False),
    StructField("loai", StringType(), False),
    StructField("so_tien", DoubleType(), False),
    StructField("so_du_truoc_nguon", DoubleType(), True),
    StructField("so_du_sau_nguon", DoubleType(), True),
    StructField("so_du_truoc_dich", DoubleType(), True),
    StructField("so_du_sau_dich", DoubleType(), True),
    StructField("kenh", StringType(), True),
    StructField("ma_thiet_bi", StringType(), True),
    StructField("dia_chi_ip", StringType(), True),
    StructField("is_fraud_ground_truth", IntegerType(), True),
] + [StructField(f"v{i}", DoubleType(), True) for i in range(1, 29)]

KAFKA_TX_SCHEMA = StructType(kafka_fields)

# 2.4.2. Bảng silver.trang_thai_khach_hang (Trạng thái cập nhật liên tục bằng MERGE từ streaming)
SILVER_TRANG_THAI_SCHEMA = StructType([
    StructField("tai_khoan", StringType(), False),
    StructField("so_gd_1h", IntegerType(), False),
    StructField("so_gd_24h", IntegerType(), False),
    StructField("so_gd_7d", IntegerType(), False),
    StructField("tong_tien_24h", DoubleType(), False),
    StructField("so_tien_tb_30d", DoubleType(), False),
    StructField("so_tien_dolech_30d", DoubleType(), False),
    StructField("so_tk_dich_khac_nhau_24h", IntegerType(), False),
    StructField("gio_giao_dich_pho_bien", ArrayType(IntegerType()), True),
    StructField("lan_gd_gan_nhat", TimestampType(), True),
    StructField("cap_nhat_luc", TimestampType(), True),
])

# Struct cho cac_yeu_to_chinh: ARRAY<STRUCT<ten:STRING, dong_gop:DOUBLE>>
CONTRIBUTING_FACTOR_STRUCT = StructType([
    StructField("ten", StringType(), False),
    StructField("dong_gop", DoubleType(), False)
])

# 2.4.3. Bảng gold.diem_rui_ro (Lưu kết quả chấm điểm, bật Change Data Feed)
GOLD_DIEM_RUI_RO_SCHEMA = StructType([
    StructField("ma_giao_dich", StringType(), False),
    StructField("diem_rui_ro", DoubleType(), False),
    StructField("xep_hang_trong_ngay", IntegerType(), True),
    StructField("cac_yeu_to_chinh", ArrayType(CONTRIBUTING_FACTOR_STRUCT), True),
    StructField("phien_ban_mo_hinh", StringType(), False),
    StructField("do_tre_ms", LongType(), False),
    StructField("trang_thai_tham_dinh", StringType(), False),  # CHUA_THAM_DINH | GIAN_LAN | HOP_LE
    StructField("thoi_diem", TimestampType(), True),
    StructField("ngay", DateType(), True),
    StructField("so_tien", DoubleType(), True),
    StructField("tai_khoan_nguon", StringType(), True),
    StructField("tai_khoan_dich", StringType(), True),
    StructField("loai", StringType(), True),
    StructField("kenh", StringType(), True),
    StructField("ma_thiet_bi", StringType(), True),
    StructField("dia_chi_ip", StringType(), True),
])

# 2.4.4. Bảng silver.ket_luan_tham_dinh (Lưu nhãn phản hồi từ chuyên gia)
SILVER_KET_LUAN_SCHEMA = StructType([
    StructField("ma_giao_dich", StringType(), False),
    StructField("nhan", IntegerType(), False),  # 0: Hợp lệ, 1: Gian lận
    StructField("nguoi_tham_dinh", StringType(), False),
    StructField("thoi_diem_ket_luan", TimestampType(), False),
    StructField("ly_do", StringType(), True),
    StructField("thoi_gian_xu_ly_giay", IntegerType(), True),
])

# 2.4.5. Bảng gold.theo_doi_drift (Giám sát trôi dạt mô hình)
GOLD_THEO_DOI_DRIFT_SCHEMA = StructType([
    StructField("ngay", DateType(), False),
    StructField("ten_dac_trung", StringType(), False),
    StructField("psi", DoubleType(), False),
    StructField("ks_statistic", DoubleType(), False),
    StructField("muc_do", StringType(), False),  # on_dinh | canh_bao | nghiem_trong
])
