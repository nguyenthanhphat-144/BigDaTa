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
            print(f"[Generator] Dang nap du lieu tu {self.csv_path}...")
            df = pd.read_csv(self.csv_path)
            df["Amount"] = df["Amount"].apply(lambda x: max(float(x), 0.01))
            _GLOBAL_CREDITCARD_DF = df
            self.df = _GLOBAL_CREDITCARD_DF
            print(f"[Generator] Da nap {len(self.df):,} dong giao dich tu creditcard.csv.")
        else:
            print(f"[Warning] Khong tim thay {self.csv_path}. Se sinh du lieu gia lap.")

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
                print(f"[Generator] KET NOI KAFKA THANH CONG tai {KAFKA_BOOTSTRAP_SERVERS}!")
                return p
            except Exception as e:
                if not wait_forever:
                    raise
                print(f"[CANH BAO] Chua ket noi duoc Kafka broker ({KAFKA_BOOTSTRAP_SERVERS})!")
                print("  -> Vui long kiem tra container Kafka trong Docker.")
                print("  -> Dang tu dong kiem tra lai sau 5 giay... (Nhan Ctrl+C de dung)\n")
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
    print(" CUA SO 4 - DANG CHAY FILE: d:\\BigDaTa2\\data\\generator.py")
    print(" VAI TRO : Bo phat giao dich the truc tuyen (PaySim / creditcard.csv) vao Kafka")
    print("=" * 80)
    parser = argparse.ArgumentParser(description="Kafka Transaction Generator (PaySim / ULB Credit Card)")
    parser.add_argument("--turbo", action="store_true", help="Chay toc do cao 300 tx/s")
    parser.add_argument("--records", type=int, default=60, help="So ban ghi phat moi dot")
    parser.add_argument("--tps", type=int, default=300, help="Toc do muc tieu tx/s")
    parser.add_argument("--auto", action="store_true", help="Tu dong phat lien tuc")
    args = parser.parse_args()

    gen = TransactionGenerator()

    # Neu truyen co --auto truc tiep tu dong lenh
    if args.auto:
        print("\n[CUA SO 4] BAT DAU CHE DO PHAT TU DONG LIEN TUC VAO KAFKA...")
        print(" (De dung phat, nhan phim Ctrl + C bat cu luc nao)\n")
        batch_idx = 1
        while True:
            try:
                print(f"\n>>> [DOT {batch_idx}] Dang phat {args.records} giao dich moi vao Kafka topic '{KAFKA_TOPIC_TRANSACTIONS}'...")
                res = gen.stream_to_kafka(num_records=args.records, delay_sec=0.03, wait_forever=True, verbose=True)
                print(f">>> [DOT {batch_idx} XONG] Da gui {res['total_sent']} GD | Gian lan: {res['fraud_count']} | Lech thu tu: {res['out_of_order_count']} | Toc do: {res['tps']} GD/s")
                batch_idx += 1
                time.sleep(3)
            except KeyboardInterrupt:
                print("\n[CUA SO 4] Da dung phat giao dich.")
                break
    elif args.turbo:
        print("\n[CUA SO 4] BAT DAU CHE DO KIEM THU TAI CAO (TURBO 300 GD/giay)...")
        res = gen.stream_to_kafka_turbo(target_tps=args.tps, total_records=args.records, wait_forever=True)
        print(f">>> [TURBO XONG] Tong gui: {res['total_sent']} GD | Toc do: {res['tps']} GD/giay")
    else:
        # BANG DIEU KHIEN TUONG TAC (DANH CHO BAO VE DO AN)
        print("\n" + "=" * 80)
        print(" [SAN SANG] DA KET NOI KAFKA BROKER (localhost:9092) - TOPIC: transactions")
        print(" HUONG DAN DIEU KHIEN:")
        print("   -> Nhan [ENTER]        : Ban 1 dot (60 giao dich) vao Kafka de kiem thu")
        print("   -> Nhap so (VD: 100)   : Ban so luong giao dich tuy chon")
        print("   -> Nhap 'auto'         : Bat che do tu dong ban lien tuc (3s mot dot)")
        print("   -> Nhap 'turbo'        : Bat che do kiem thu tai cao (300 GD/giay)")
        print("   -> Nhap 'q'            : Thoat chuong trinh")
        print(" (*) MEO: Ban cung co the bam nut 'Chay Buoc 01 -> 06: Phat giao dich & Cham diem'")
        print("          truc tiep tren giao dien Web UI (http://127.0.0.1:5000)!")
        print("          Cua so 4 se tu dong nhan lenh va hien thi truc tiep len man hinh.")
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
                prompt_msg = f"[DOT {batch_no}] Nhan ENTER de phat 60 GD (hoac go 'auto', 'turbo', 'q'): "
                action, payload = wait_console_or_web_trigger(prompt_msg, trigger_file)

                if action == "trigger":
                    n_rec = int(payload.get("num_records", 60))
                    trig_id = payload.get("id", f"TRIG_{int(time.time()*1000)}")
                    print("\n" + "=" * 80)
                    print(f" [TIN HIEU TU GIAO DIEN WEB] Nhan lenh tu nut bam Web UI:")
                    print(f" >>> Dang truc tiep phat {n_rec} giao dich vao Kafka topic '{KAFKA_TOPIC_TRANSACTIONS}'...")
                    print("=" * 80)
                    res = gen.stream_to_kafka(num_records=n_rec, delay_sec=0.02, wait_forever=True, verbose=True)
                    print(f">>> [DOT {batch_no} XONG - TU WEB UI] Da gui {res['total_sent']} GD | Gian lan: {res['fraud_count']} | Lech thu tu: {res['out_of_order_count']} | Toc do: {res['tps']} GD/giay\n")
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
                    print("\n[CUA SO 4] Da thoat chuong trinh phat giao dich.")
                    break
                elif cmd == "turbo":
                    print(f"\n>>> [TURBO] Dang phat 600 giao dich voi toc do cao 300 GD/giay...")
                    res = gen.stream_to_kafka_turbo(target_tps=300, total_records=600, wait_forever=True)
                    print(f">>> [HOAN TAT] Tong gui: {res['total_sent']} GD | Toc do: {res['tps']} GD/giay\n")
                    batch_no += 1
                elif cmd == "auto":
                    print("\n>>> [CHE DO TU DONG] Dang ban lien tuc vao Kafka (Nhan Ctrl+C de tam dung)...")
                    sub_idx = 1
                    try:
                        while True:
                            print(f"\n--- [Tu dong - Lan {sub_idx}] Dang phat 60 giao dich moi... ---")
                            res = gen.stream_to_kafka(num_records=60, delay_sec=0.02, wait_forever=True, verbose=True)
                            print(f"--- Da gui {res['total_sent']} GD. Tam nghi 3 giay... ---")
                            sub_idx += 1
                            time.sleep(3)
                    except KeyboardInterrupt:
                        print("\n>>> Da tam dung che do tu dong. Tro ve bang dieu khien thu cong.\n")
                    batch_no += sub_idx
                else:
                    n_rec = 60
                    if cmd.isdigit():
                        n_rec = max(1, int(cmd))
                    print(f"\n>>> Dang phat {n_rec} giao dich the vao Kafka topic '{KAFKA_TOPIC_TRANSACTIONS}'...")
                    res = gen.stream_to_kafka(num_records=n_rec, delay_sec=0.03, wait_forever=True, verbose=True)
                    print(f">>> [DOT {batch_no} XONG] Da gui {res['total_sent']} GD | Gian lan: {res['fraud_count']} | Lech thu tu: {res['out_of_order_count']} | Toc do: {res['tps']} GD/giay\n")
                    batch_no += 1
            except KeyboardInterrupt:
                print("\n[CUA SO 4] Da dung chuong trinh.")
                break
            except Exception as err:
                print(f"[CUA SO 4 LOI] {err}\n")
