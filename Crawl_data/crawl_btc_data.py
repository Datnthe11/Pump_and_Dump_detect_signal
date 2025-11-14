# btc_ohlcv_historical.py
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone

def fetch_ohlcv_binance(symbol="BTCUSDT", interval="1m", start_time=None, end_time=None):
    base_url = "https://api.binance.com/api/v3/klines"
    limit = 1000  # Max records per request
    all_data = []

    while start_time < end_time:
        try:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": int(start_time.timestamp() * 1000),
                "limit": limit
            }

            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data:
                print("✅ Finished crawling — no more data.")
                break

            if isinstance(data, dict) and data.get("code"):
                print("⚠️ API Error:", data)
                break

            all_data += data

            # Move to next batch
            last_time = int(data[-1][0]) / 1000
            start_time = datetime.fromtimestamp(last_time + 60, tz=timezone.utc)  # +1m, aware timestamp

            time.sleep(0.5)  # polite delay

        except requests.exceptions.RequestException as e:
            print("⚠️ Request error, waiting 10s:", e)
            time.sleep(10)
            continue

    # Format to DataFrame
    columns = ["open_time", "open", "high", "low", "close", "volume",
               "close_time", "quote_asset_volume", "num_trades",
               "taker_buy_base", "taker_buy_quote", "ignore"]

    df = pd.DataFrame(all_data, columns=columns)

    # Format types
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["symbol"] = symbol

    float_cols = ["open", "high", "low", "close", "volume"]
    df[float_cols] = df[float_cols].astype(float)

    return df[["open_time", "symbol", "open", "high", "low", "close", "volume"]]

if __name__ == "__main__":
    # Define time range (timezone-aware)
    start_dt = datetime(2025, 7, 1, tzinfo=timezone.utc)
    end_dt = datetime(2025, 9, 30, 23, 59, tzinfo=timezone.utc)

    df = fetch_ohlcv_binance("BTCUSDT", "1m", start_dt, end_dt)

    # Save
    out_file = "btc_ohlcv_1m_2025_Q3.csv"
    df.to_csv(out_file, index=False)
    print(f"✅ Saved {len(df)} rows to {out_file}")



def fetch_orderbook_snapshot(symbol="BTCUSDT", limit=5):
    url = "https://api.binance.com/api/v3/depth"
    params = {
        "symbol": symbol.upper(),
        "limit": limit
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # phát hiện lỗi HTTP
        data = response.json()

        bid_price, bid_qty = map(float, data["bids"][0])
        ask_price, ask_qty = map(float, data["asks"][0])
        spread = ask_price - bid_price
        bid_ask_ratio = bid_qty / ask_qty if ask_qty != 0 else 0

        return {
            "timestamp": datetime.now(timezone.utc),   # dùng timezone-aware
            "symbol": symbol,
            "top_bid_price": bid_price,
            "top_ask_price": ask_price,
            "spread": spread,
            "bid_ask_ratio": bid_ask_ratio
        }
    except Exception as e:
        print("⚠️ Error fetching snapshot:", e)
        return None

def crawl_orderbook_over_period(symbol="BTCUSDT", start_dt=None, end_dt=None, freq_minutes=5):
    current = start_dt
    records = []

    while current <= end_dt:
        print(f"📥 Fetching {symbol} OrderBook snapshot at {current}")
        snapshot = fetch_orderbook_snapshot(symbol)
        if snapshot:
            snapshot["snapshot_time"] = current
            records.append(snapshot)

        current += timedelta(minutes=freq_minutes)
        time.sleep(0.5)  # tăng delay để tránh bị block

    return pd.DataFrame(records)

if __name__ == "__main__":
    start = datetime(2025, 7, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 9, 30, 23, 55, tzinfo=timezone.utc)

    df = crawl_orderbook_over_period("BTCUSDT", start, end, freq_minutes=5)

    df.to_csv("btc_orderbook_snapshot_5m_2025_Q3.csv", index=False)
    print(f"✅ Saved {len(df)} rows to btc_orderbook_snapshot_5m_2025_Q3.csv")


def clean_csv_remove_symbol(input_path, output_path):
    try:
        df = pd.read_csv(input_path)

        if "symbol" in df.columns:
            df = df.drop(columns=["symbol"])
            df.to_csv(output_path, index=False)
            print(f"✅ Cleaned file saved as: {output_path}")
        else:
            print(f"⚠️ Column 'symbol' not found in: {input_path}")
    except Exception as e:
        print(f"❌ Failed to process {input_path}: {e}")

if __name__ == "__main__":
    # File OHLCV
    clean_csv_remove_symbol(
        input_path="/Users/tavantai/Developer/codepy/btc_ohlcv_1m_2025_10_11.csv",
        output_path="btc_ohlcv_new.csv"
    )

    # File OrderBook
    clean_csv_remove_symbol(
        input_path="/Users/tavantai/Developer/codepy/btc_orderbook_snapshot_5m_2025_10_11.csv",
        output_path="btc_orderbook_new.csv"
    )

# Đọc file CSV gốc
df = pd.read_csv("/Users/tavantai/Developer/codepy/btc_orderbook_snapshot_5m_2025_10_11.csv")

# Xoá cột 'timestamp' nếu tồn tại
if "timestamp" in df.columns:
    df.drop(columns=["timestamp"], inplace=True)

# Ghi đè lại file gốc
df.to_csv("/Users/tavantai/Developer/codepy/btc_orderbook_snapshot_5m_2025_10_11.csv", index=False)

print("✅ Đã xóa 'timestamp' và ghi đè file btc_orderbook.csv")


# Đọc file gốc
df = pd.read_csv("/Users/tavantai/Developer/codepy/btc_orderbook_snapshot_5m_2025_10_11.csv")

# Chuyển snapshot_time về dạng chuỗi ISO không chứa offset +00:00
if "snapshot_time" in df.columns:
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

# Ghi đè file
df.to_csv("/Users/tavantai/Developer/codepy/btc_orderbook_snapshot_5m_2025_10_11.csv", index=False)

print("✅ Đã chuẩn hóa snapshot_time và ghi đè file btc_orderbook.csv")


# Đọc dữ liệu
ohlcv = pd.read_csv("/Users/tavantai/Developer/codepy/cleandata/btc_ohlcv_new.csv", parse_dates=["open_time"])
orderbook = pd.read_csv("/Users/tavantai/Developer/codepy/cleandata/btc_orderbook_new.csv", parse_dates=["snapshot_time"])

# Gắn mỗi dòng OHLCV với snapshot gần nhất trước đó (hoặc đúng bằng snapshot_time)
merged = pd.merge_asof(
    ohlcv.sort_values("open_time"),
    orderbook.sort_values("snapshot_time"),
    left_on="open_time",
    right_on="snapshot_time",
    direction="backward"  # ghép với snapshot gần nhất trước đó
)

# Lưu file mới
merged.to_csv("merged_btc_ohlcv_orderbook_new.csv", index=False)
print("✅ Đã lưu merged_btc_ohlcv_orderbook.csv")


# Đọc file CSV gốc
df = pd.read_csv("/Users/tavantai/Developer/codepy/merged_btc_ohlcv_orderbook_new.csv")

# Xoá cột 'snapshot_time' nếu tồn tại
if "snapshot_time" in df.columns:
    df.drop(columns=["snapshot_time"], inplace=True)

# Ghi ra một file mới (đổi tên)
output_path = "/Users/tavantai/Developer/codepy/cleandata/merged_btc_ohlcv_orderbook_official_new.csv"
df.to_csv(output_path, index=False)

print(f"✅ Đã tạo file mới: {output_path}")


def fetch_orderbook_snapshot(symbol="BTCUSDT", limit=5):
    url = "https://api.binance.com/api/v3/depth"
    params = {
        "symbol": symbol.upper(),
        "limit": limit
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        bid_price, bid_qty = map(float, data["bids"][0])
        ask_price, ask_qty = map(float, data["asks"][0])
        spread = ask_price - bid_price
        bid_ask_ratio = bid_qty / ask_qty if ask_qty != 0 else 0

        return {
            "timestamp": datetime.now(timezone.utc),
            "symbol": symbol,
            "top_bid_price": bid_price,
            "top_ask_price": ask_price,
            "spread": spread,
            "bid_ask_ratio": bid_ask_ratio
        }
    except Exception as e:
        print("⚠️ Error fetching snapshot:", e)
        return None

def crawl_orderbook_over_period(symbol="BTCUSDT", start_dt=None, end_dt=None, freq_minutes=5):
    current = start_dt
    records = []

    while current <= end_dt:
        print(f"📥 Fetching {symbol} OrderBook snapshot at {current}")
        snapshot = fetch_orderbook_snapshot(symbol)
        if snapshot:
            snapshot["snapshot_time"] = current
            records.append(snapshot)

        current += timedelta(minutes=freq_minutes)
        time.sleep(0.5)

    return pd.DataFrame(records)

if __name__ == "__main__":
    # Crawl 1 day only: 11 October 2025
    start = datetime(2025, 10, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 10, 11, 23, 55, tzinfo=timezone.utc)

    df = crawl_orderbook_over_period("BTCUSDT", start, end, freq_minutes=5)

    out_file = "btc_orderbook_snapshot_5m_2025_10_11.csv"
    df.to_csv(out_file, index=False)
    print(f"✅ Saved {len(df)} rows to {out_file}")

import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone

def fetch_ohlcv_binance(symbol="BTCUSDT", interval="1m", start_time=None, end_time=None):
    base_url = "https://api.binance.com/api/v3/klines"
    limit = 1000  # Max records per request
    all_data = []

    while start_time < end_time:
        try:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": int(start_time.timestamp() * 1000),
                "limit": limit
            }

            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data:
                print("✅ Finished crawling — no more data.")
                break

            if isinstance(data, dict) and data.get("code"):
                print("⚠️ API Error:", data)
                break

            all_data += data

            # Move to next batch
            last_time = int(data[-1][0]) / 1000
            start_time = datetime.fromtimestamp(last_time + 60, tz=timezone.utc)  # move +1 minute

            time.sleep(0.5)  # polite delay

        except requests.exceptions.RequestException as e:
            print("⚠️ Request error, waiting 10s:", e)
            time.sleep(10)
            continue

    # Format to DataFrame
    columns = ["open_time", "open", "high", "low", "close", "volume",
               "close_time", "quote_asset_volume", "num_trades",
               "taker_buy_base", "taker_buy_quote", "ignore"]

    df = pd.DataFrame(all_data, columns=columns)

    # Format types
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["symbol"] = symbol

    float_cols = ["open", "high", "low", "close", "volume"]
    df[float_cols] = df[float_cols].astype(float)

    return df[["open_time", "symbol", "open", "high", "low", "close", "volume"]]

if __name__ == "__main__":
    # Crawl 1 day only: 11 October 2025 (UTC)
    start_dt = datetime(2025, 1, 7, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(2025, 9, 30, 23, 59, tzinfo=timezone.utc)

    df = fetch_ohlcv_binance("BTCUSDT", "1m", start_dt, end_dt)

    # Save
    out_file = "btc_ohlcv_1m_2025_10_11.csv"
    df.to_csv(out_file, index=False)
    print(f"✅ Saved {len(df)} rows to {out_file}")