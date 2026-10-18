import os
import sys
import time
import json
import random
import uuid
import datetime
import glob
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRANSACTIONS,
    CREDITCARD_CSV, TMP_DIR, log_pipeline_event, set_console_title
)


_GLOBAL_CREDITCARD_DF = None


class TransactionGenerator:
    """Reads creditcard.csv and enriches records with PaySim-compatible behavioral fields
    (tai_khoan_nguon, tai_khoan_dich, loai, kenh, balances, device/IP).
    Replays by step (1 simulated hour = 10 real seconds) at up to 300 tx/sec
    and injects out-of-order transactions (up to 30 seconds late) to test Watermark.
    """

    LOAI_GIAO_DICH = ["CHUYEN_KHOAN", "RUT_TIEN", "THANH_TOAN", "NAP_TIEN"]
    KENH_GIAO_DICH = ["ATM", "POS", "ONLINE", "APP"]

    def __init__(self, csv_path=CREDITCARD_CSV, num_accounts=2000, num_merchants=500):
        self.csv_path = csv_path
        self.num_accounts = num_accounts
        self.num_merchants = num_merchants
        self.accounts = [f"ACC_{100000 + i}" for i in range(num_accounts)]
        self.merchants = [f"MERCH_{200000 + i}" for i in range(num_merchants)]
        self.devices = [f"DEV_{1000 + i}" for i in range(500)]
        self.df = None
        self._load_dataset()

    def _load_dataset(self):
        """Loads creditcard.csv from d:\\BigDaTa2\\creditcard.csv with global memory caching."""
        global _GLOBAL_CREDITCARD_DF
        if _GLOBAL_CREDITCARD_DF is not None:
            self.df = _GLOBAL_CREDITCARD_DF
            return

        if os.path.exists(self.csv_path):
            print(f"[Generator] Đang nạp dữ liệu từ {self.csv_path}...")
            df = pd.read_csv(self.csv_path)
            df["Amount"] = df["Amount"].apply(lambda x: max(float(x), 0.01))
            _GLOBAL_CREDITCARD_DF = df
            self.df = _GLOBAL_CREDITCARD_DF
            print(f"[Generator] Đã nạp {len(self.df):,} dòng giao dịch từ creditcard.csv.")
        else:
            print(f"[Warning] Không tìm thấy {self.csv_path}. Sẽ sinh dữ liệu giả lập.")

    def build_transaction_record(self, row, current_time=None, simulate_out_of_order=True):
        """Builds a transaction record conforming to bronze.giao_dich & Kafka schema."""
        now = current_time or datetime.datetime.now()

        # Simulate out-of-order arrival up to 30 seconds for Watermark testing
        is_delayed = simulate_out_of_order and (random.random() < 0.18)
        if is_delayed:
            delay_seconds = random.uniform(5.0, 30.0)
            tx_time = now - datetime.timedelta(seconds=delay_seconds)
        else:
            tx_time = now - datetime.timedelta(seconds=random.uniform(0.05, 1.2))

        is_fraud = int(row.get("Class", 0))
        account = random.choice(self.accounts[:150] if is_fraud else self.accounts)
        merchant = random.choice(self.merchants)
        loai = random.choice(["CHUYEN_KHOAN", "RUT_TIEN"] if is_fraud else self.LOAI_GIAO_DICH)
        kenh = random.choice(["ONLINE", "APP"] if is_fraud else self.KENH_GIAO_DICH)

        amount = float(row.get("Amount", random.uniform(15.0, 5000.0)))
        if is_fraud and amount < 50.0:
            amount = round(random.uniform(450.0, 4800.0), 2)
        amount = max(amount, 0.01)

        bal_before_src = round(random.uniform(amount * 1.05, amount * 8 + 3000), 2)
        bal_after_src = round(max(0.0, bal_before_src - amount), 2)
        bal_before_dst = round(random.uniform(200.0, 45000.0), 2)
        bal_after_dst = round(bal_before_dst + amount, 2)

        tx = {
            "ma_giao_dich": f"TX_{uuid.uuid4().hex[:10].upper()}",
            "thoi_diem": tx_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "thoi_diem_nhan": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "tai_khoan_nguon": account,
            "tai_khoan_dich": merchant,
            "loai": loai,
            "so_tien": round(amount, 2),
            "so_du_truoc_nguon": bal_before_src,
            "so_du_sau_nguon": bal_after_src,
            "so_du_truoc_dich": bal_before_dst,
            "so_du_sau_dich": bal_after_dst,
            "kenh": kenh,
            "ma_thiet_bi": f"DEV_STRANGE_{random.randint(900,999)}" if is_fraud else random.choice(self.devices),
            "dia_chi_ip": f"185.220.{random.randint(1, 254)}.{random.randint(1, 254)}" if is_fraud else f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}",
            "is_fraud_ground_truth": is_fraud,
            "is_out_of_order": bool(is_delayed)
        }

        for i in range(1, 29):
            col = f"V{i}"
            tx[f"v{i}"] = float(row.get(col, random.gauss(0, 1)))

        return tx

    def _get_kafka_producer(self, linger_ms=5, batch_size=32768, wait_forever=False):
        from kafka import KafkaProducer
        while True:
            try:
                p = KafkaProducer(
                    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    linger_ms=linger_ms,
                    batch_size=batch_size,
                    request_timeout_ms=4000
                )
                print(f"[Generator] KẾT NỐI KAFKA THÀNH CÔNG tại {KAFKA_BOOTSTRAP_SERVERS}!")
                return p
            except Exception as e:
                if not wait_forever:
                    raise
                print(f"[CẢNH BÁO] Chưa kết nối được Kafka broker ({KAFKA_BOOTSTRAP_SERVERS})!")
                print("  -> Vui lòng MỞ ỨNG DỤNG DOCKER DESKTOP để kích hoạt Kafka container.")
                print("  -> Đang tự động kiểm tra lại sau 5 giây... (Nhấn Ctrl+C để dừng)\n")
                time.sleep(5)

    def stream_to_kafka(self, num_records=500, delay_sec=0.01, fraud_ratio=0.08, topic=KAFKA_TOPIC_TRANSACTIONS, wait_forever=False, verbose=False):
        """Streams transactions to Kafka topic 'transactions' with step compression (1h = 10s)
        and out-of-order simulation (up to 30s).
        """
        producer = self._get_kafka_producer(linger_ms=5, batch_size=16384, wait_forever=wait_forever)

        fraud_rows = self.df[self.df["Class"] == 1] if self.df is not None else pd.DataFrame()
        normal_rows = self.df[self.df["Class"] == 0] if self.df is not None else pd.DataFrame()

        total_sent = 0
        fraud_count = 0
        out_of_order_count = 0
        t0 = time.time()

        try:
            for i in range(num_records):
                if len(fraud_rows) > 0 and (random.random() < fraud_ratio or i % 15 == 0):
                    row = fraud_rows.sample(1).iloc[0].to_dict()
                elif len(normal_rows) > 0:
                    row = normal_rows.sample(1).iloc[0].to_dict()
                else:
                    row = {"Amount": random.uniform(20, 1500), "Class": 0}

                tx = self.build_transaction_record(row, simulate_out_of_order=True)
                if tx.get("is_out_of_order"):
                    out_of_order_count += 1
                if tx["is_fraud_ground_truth"] == 1:
                    fraud_count += 1

                producer.send(topic, tx)
                total_sent += 1

                if verbose or (i + 1) % 10 == 0:
                    tag = "[CANH BAO GIAN LAN]" if tx["is_fraud_ground_truth"] == 1 else "[HOP LE]"
                    print(f"  [{i+1:03d}/{num_records}] {tx['thoi_diem']} | MaGD: {tx['ma_giao_dich']} | ${tx['so_tien']:,.2f} | {tx['loai']:<12} | {tag} -> KAFKA ({topic})")

                if delay_sec > 0:
                    time.sleep(delay_sec)

            producer.flush()
            elapsed = max(0.01, time.time() - t0)
            tps = round(total_sent / elapsed, 1)

            log_pipeline_event(
                "Bộ phát Kafka (creditcard.csv)",
                f"Kafka Topic: {topic}",
                f"Phát {total_sent} giao dịch (Step 1h=10s, Tốc độ {tps} tx/s)",
                f"Gian lận gốc: {fraud_count} | Giao dịch lệch thứ tự (Out-of-order <=30s): {out_of_order_count}",
                count=total_sent
            )
            return {
                "total_sent": total_sent,
                "fraud_count": fraud_count,
                "out_of_order_count": out_of_order_count,
                "elapsed_sec": round(elapsed, 2),
                "tps": tps
            }
        finally:
            producer.close()

    def stream_to_kafka_turbo(self, target_tps=300, total_records=1500, out_of_order_ratio=0.18, topic=KAFKA_TOPIC_TRANSACTIONS, wait_forever=True):
        """High-throughput Turbo Mode: Simulates banking peak load (~300 tx/sec) with 1h=10s replay."""
        producer = self._get_kafka_producer(linger_ms=5, batch_size=32768, wait_forever=wait_forever)

        fraud_rows = self.df[self.df["Class"] == 1] if self.df is not None else pd.DataFrame()
        normal_rows = self.df[self.df["Class"] == 0] if self.df is not None else pd.DataFrame()

        total_sent = 0
        fraud_count = 0
        delayed_count = 0
        start_time = time.time()
        batch_slice = max(15, int(target_tps / 10))

        try:
            while total_sent < total_records:
                batch_start = time.time()
                current_batch_size = min(batch_slice, total_records - total_sent)

                for _ in range(current_batch_size):
                    if len(fraud_rows) > 0 and (random.random() < 0.06 or total_sent % 20 == 0):
                        row = fraud_rows.sample(1).iloc[0].to_dict()
                    elif len(normal_rows) > 0:
                        row = normal_rows.sample(1).iloc[0].to_dict()
                    else:
                        row = {"Amount": random.uniform(10, 1000), "Class": 0}

                    is_delayed = random.random() < out_of_order_ratio
                    tx = self.build_transaction_record(row, simulate_out_of_order=is_delayed)
                    if is_delayed:
                        delayed_count += 1
                    if tx["is_fraud_ground_truth"] == 1:
                        fraud_count += 1

                    producer.send(topic, tx)
                    total_sent += 1

                elapsed_batch = time.time() - batch_start
                target_batch_duration = current_batch_size / float(target_tps)
                if elapsed_batch < target_batch_duration:
                    time.sleep(target_batch_duration - elapsed_batch)

            producer.flush()
            total_duration = max(0.01, time.time() - start_time)
            avg_tps = round(total_sent / total_duration, 1)

            log_pipeline_event(
                "Bộ phát Kafka Turbo (300 tx/s)",
                f"Kafka Topic: {topic}",
                f"Stress Test giờ cao điểm: {total_sent} GD đạt {avg_tps} tx/s",
                f"Gian lận: {fraud_count} | Lệch thứ tự (Out-of-order <=30s): {delayed_count}",
                count=total_sent
            )
            return {
                "total_sent": total_sent,
                "fraud_count": fraud_count,
                "out_of_order_count": delayed_count,
                "elapsed_sec": round(total_duration, 2),
                "tps": avg_tps
            }
        finally:
            producer.close()


def wait_console_or_web_trigger(prompt_text, trigger_file):
    """Waits for user input in console OR a trigger JSON file from Web UI."""
    has_msvcrt = False
    try:
        import msvcrt
        has_msvcrt = True
    except ImportError:
        pass

    if not has_msvcrt or not sys.stdin.isatty():
        print(prompt_text, end="", flush=True)
        while True:
            if os.path.exists(trigger_file):
                try:
                    with open(trigger_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    os.remove(trigger_file)
                    print()
                    return ("trigger", data)
                except Exception:
                    pass
            time.sleep(0.15)

    sys.stdout.write(prompt_text)
    sys.stdout.flush()
    chars = []

    while True:
        # Check trigger from Web UI
        if os.path.exists(trigger_file):
            try:
                with open(trigger_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                os.remove(trigger_file)
                # Clear current line visually
                sys.stdout.write("\r" + " " * 90 + "\r")
                sys.stdout.flush()
                return ("trigger", data)
            except Exception:
                pass

        # Check console keystrokes
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch in ("\r", "\n"):
                line = "".join(chars).strip()
                sys.stdout.write("\n")
                sys.stdout.flush()
                return ("input", line)
            elif ch == "\x03":  # Ctrl+C
                raise KeyboardInterrupt
            elif ch in ("\b", "\x7f"):  # Backspace
                if chars:
                    chars.pop()
                    sys.stdout.write("\b \b")
                    sys.stdout.flush()
            elif ord(ch) >= 32:  # Printable character
                chars.append(ch)
                sys.stdout.write(ch)
                sys.stdout.flush()

        time.sleep(0.06)


if __name__ == "__main__":
    import argparse
    set_console_title("[CUA SO 4 - KAFKA PRODUCER] Phat giao dich the vao Kafka")
    print("=" * 80)
    print(" CỬA SỐ 4 — ĐANG CHẠY FILE: d:\\BigDaTa2\\data\\generator.py")
    print(" VAI TRÒ : Bộ phát giao dịch thẻ trực tuyến (PaySim / creditcard.csv) vào Kafka")
    print("=" * 80)
    parser = argparse.ArgumentParser(description="Kafka Transaction Generator (PaySim / ULB Credit Card)")
    parser.add_argument("--turbo", action="store_true", help="Chạy tốc độ cao 300 tx/s")
    parser.add_argument("--records", type=int, default=60, help="Số bản ghi phát mỗi đợt")
    parser.add_argument("--tps", type=int, default=300, help="Tốc độ mục tiêu tx/s")
    parser.add_argument("--auto", action="store_true", help="Tự động phát liên tục")
    args = parser.parse_args()

    gen = TransactionGenerator()

    # Nếu truyền cờ --auto trực tiếp từ dòng lệnh
    if args.auto:
        print("\n[CỬA SỐ 4] BẮT ĐẦU CHẾ ĐỘ PHÁT TỰ ĐỘNG LIÊN TỤC VÀO KAFKA...")
        print(" (Để dừng phát, nhấn phím Ctrl + C bất cứ lúc nào)\n")
        batch_idx = 1
        while True:
            try:
                print(f"\n>>> [ĐỢT {batch_idx}] Đang phát {args.records} giao dịch mới vào Kafka topic '{KAFKA_TOPIC_TRANSACTIONS}'...")
                res = gen.stream_to_kafka(num_records=args.records, delay_sec=0.03, wait_forever=True, verbose=True)
                print(f">>> [ĐỢT {batch_idx} XONG] Đã gửi {res['total_sent']} GD | Gian lận: {res['fraud_count']} | Lệch thứ tự: {res['out_of_order_count']} | Tốc độ: {res['tps']} GD/s")
                batch_idx += 1
                time.sleep(3)
            except KeyboardInterrupt:
                print("\n[CỬA SỐ 4] Đã dừng phát giao dịch.")
                break
    elif args.turbo:
        print("\n[CỬA SỐ 4] BẮT ĐẦU CHẾ ĐỘ KIỂM THỬ TẢI CAO (TURBO 300 GD/giây)...")
        res = gen.stream_to_kafka_turbo(target_tps=args.tps, total_records=args.records, wait_forever=True)
        print(f">>> [TURBO XONG] Tổng gửi: {res['total_sent']} GD | Tốc độ: {res['tps']} GD/giây")
    else:
        # BẢNG ĐIỀU KHIỂN TƯƠNG TÁC (DÀNH CHO BẢO VỆ ĐỒ ÁN)
        print("\n" + "=" * 80)
        print(" [SẴN SÀNG] ĐÃ KẾT NỐI KAFKA BROKER (localhost:9092) — TOPIC: transactions")
        print(" HƯỚNG DẪN ĐIỀU KHIỂN:")
        print("   -> Nhấn [ENTER]        : Bắn 1 đợt (60 giao dịch) vào Kafka để kiểm thử")
        print("   -> Nhập số (VD: 100)   : Bắn số lượng giao dịch tùy chọn")
        print("   -> Nhập 'auto'         : Bật chế độ tự động bắn liên tục (3s một đợt)")
        print("   -> Nhập 'turbo'        : Bật chế độ kiểm thử tải cao (300 GD/giây)")
        print("   -> Nhập 'q'            : Thoát chương trình")
        print(" (*) MẸO: Bạn cũng có thể bấm nút 'Mô phỏng phát giao dịch & Chấm điểm'")
        print("          trực tiếp trên giao diện Web UI (http://127.0.0.1:5000)!")
        print("          Cửa sổ 4 sẽ tự động nhận lệnh và hiển thị trực tiếp lên màn hình.")
        print("=" * 80 + "\n")

        trigger_file = os.path.join(TMP_DIR, "web_trigger.json")
        for old_f in glob.glob(os.path.join(TMP_DIR, "web_trigger*.json")):
            try:
                os.remove(old_f)
            except Exception:
                pass

        batch_no = 1
        while True:
            try:
                prompt_msg = f"[ĐỢT {batch_no}] Nhấn ENTER để phát 60 GD (hoặc gõ 'auto', 'turbo', 'q'): "
                action, payload = wait_console_or_web_trigger(prompt_msg, trigger_file)

                if action == "trigger":
                    n_rec = int(payload.get("num_records", 60))
                    trig_id = payload.get("id", f"TRIG_{int(time.time()*1000)}")
                    print("\n" + "=" * 80)
                    print(f" [TÍN HIỆU TỪ GIAO DIỆN WEB] Nhận lệnh từ nút bấm Web UI:")
                    print(f" >>> Đang trực tiếp phát {n_rec} giao dịch vào Kafka topic '{KAFKA_TOPIC_TRANSACTIONS}'...")
                    print("=" * 80)
                    res = gen.stream_to_kafka(num_records=n_rec, delay_sec=0.02, wait_forever=True, verbose=True)
                    print(f">>> [ĐỢT {batch_no} XONG - TỪ WEB UI] Đã gửi {res['total_sent']} GD | Gian lận: {res['fraud_count']} | Lệch thứ tự: {res['out_of_order_count']} | Tốc độ: {res['tps']} GD/giây\n")
                    batch_no += 1

                    done_file = os.path.join(TMP_DIR, f"web_trigger_done_{trig_id}.json")
                    try:
                        with open(done_file, "w", encoding="utf-8") as f:
                            json.dump({"id": trig_id, "stats": res}, f)
                    except Exception:
                        pass
                    continue

                cmd = payload.lower()
                if cmd in ("q", "exit", "quit"):
                    print("\n[CỬA SỐ 4] Đã thoát chương trình phát giao dịch.")
                    break
                elif cmd == "turbo":
                    print(f"\n>>> [TURBO] Đang phát 600 giao dịch với tốc độ cao 300 GD/giây...")
                    res = gen.stream_to_kafka_turbo(target_tps=300, total_records=600, wait_forever=True)
                    print(f">>> [HOÀN TẤT] Tổng gửi: {res['total_sent']} GD | Tốc độ: {res['tps']} GD/giây\n")
                    batch_no += 1
                elif cmd == "auto":
                    print("\n>>> [CHẾ ĐỘ TỰ ĐỘNG] Đang bắn liên tục vào Kafka (Nhấn Ctrl+C để tạm dừng)...")
                    sub_idx = 1
                    try:
                        while True:
                            print(f"\n--- [Tự động - Lần {sub_idx}] Đang phát 60 giao dịch mới... ---")
                            res = gen.stream_to_kafka(num_records=60, delay_sec=0.02, wait_forever=True, verbose=True)
                            print(f"--- Đã gửi {res['total_sent']} GD. Tạm nghỉ 3 giây... ---")
                            sub_idx += 1
                            time.sleep(3)
                    except KeyboardInterrupt:
                        print("\n>>> Đã tạm dừng chế độ tự động. Trở về bảng điều khiển thủ công.\n")
                    batch_no += sub_idx
                else:
                    n_rec = 60
                    if cmd.isdigit():
                        n_rec = max(1, int(cmd))
                    print(f"\n>>> Đang phát {n_rec} giao dịch thẻ vào Kafka topic '{KAFKA_TOPIC_TRANSACTIONS}'...")
                    res = gen.stream_to_kafka(num_records=n_rec, delay_sec=0.03, wait_forever=True, verbose=True)
                    print(f">>> [ĐỢT {batch_no} XONG] Đã gửi {res['total_sent']} GD | Gian lận: {res['fraud_count']} | Lệch thứ tự: {res['out_of_order_count']} | Tốc độ: {res['tps']} GD/giây\n")
                    batch_no += 1
            except KeyboardInterrupt:
                print("\n[CỬA SỐ 4] Đã dừng chương trình.")
                break
            except Exception as err:
                print(f"[CỬA SỐ 4 LỖI] {err}\n")
