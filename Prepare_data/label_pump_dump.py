"""
Label Pump/Dump với features đa khung thời gian (1m, 5m, 15m)
"""

import pandas as pd
import numpy as np

# ==============================
# 🔧 HÀM TÍNH Z-SCORE ROLLING
# ==============================
def calculate_rolling_zscore(series, window=360, min_periods=60):
    rolling_mean = series.rolling(window=window, min_periods=min_periods).mean()
    rolling_std = series.rolling(window=window, min_periods=min_periods).std()
    rolling_std = rolling_std.where(rolling_std != 0, np.nan)
    z_score = (series - rolling_mean) / rolling_std
    return z_score.fillna(0)

# ==============================
# 🔧 HÀM TÍNH FEATURES CHO MỖI TIMEFRAME
# ==============================
def calculate_features_for_df(df, window_suffix, base_window=360):
    df = df.copy()
    
    timeframe_minutes = 1
    if window_suffix == '_5m': timeframe_minutes = 5
    elif window_suffix == '_15m': timeframe_minutes = 15
    
    window = base_window // timeframe_minutes
    min_periods = max(1, window // 6)
    
    # Price return
    price_return = pd.Series(
        np.where(df['open'] != 0, (df['close'] - df['open']) / df['open'], 0),
        index=df.index
    )
    df[f'price_return{window_suffix}'] = price_return
    
    # Z-scores
    df[f'z_return{window_suffix}'] = calculate_rolling_zscore(price_return, window, min_periods)
    df[f'z_volume{window_suffix}'] = calculate_rolling_zscore(df['volume'], window, min_periods)
    
    # Close position
    candle_body = (df['high'] - df['low']).replace(0, np.nan)
    close_position = (df['close'] - df['low']) / candle_body
    df[f'close_position{window_suffix}'] = close_position.fillna(0.5)
    
    new_cols = [
        f'price_return{window_suffix}',
        f'z_return{window_suffix}',
        f'z_volume{window_suffix}',
        f'close_position{window_suffix}'
    ]
    return df[new_cols]

# ==============================
# 🧠 GÁN NHÃN PUMP / DUMP
# ==============================
def label_anomaly_pump_dump(df):
    df = df.copy()
    
    # Kiểm tra cột bắt buộc
    required_cols = [
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'top_bid_price', 'top_ask_price', 'spread',
        'bid_ask_ratio', 'final_sentiment_score'
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"THIẾU CỘT: {missing_cols}")
    
    df['open_time'] = pd.to_datetime(df['open_time'])
    df = df.sort_values('open_time').reset_index(drop=True)
    
    # Điền NaN
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 
                    'top_bid_price', 'top_ask_price', 'spread',
                    'bid_ask_ratio', 'final_sentiment_score']
    for col in numeric_cols:
        df[col] = df[col].ffill().bfill().fillna(0)
    
    # ===== TẠO FEATURES ĐA KHUNG THỜI GIAN =====
    print("Bắt đầu tạo features đa khung thời gian...")
    df_1m = df.set_index('open_time')
    
    # 1m
    features_1m = calculate_features_for_df(df_1m, '_1m')
    df_1m = pd.concat([df_1m, features_1m], axis=1)
    
    # Resample 5m & 15m
    resample_rules = {
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
    }
    df_5m = df_1m.resample('5T').agg(resample_rules).dropna()
    df_15m = df_1m.resample('15T').agg(resample_rules).dropna()
    
    # Tạo features 5m & 15m
    features_5m = calculate_features_for_df(df_5m, '_5m')
    features_15m = calculate_features_for_df(df_15m, '_15m')
    
    # Merge tất cả
    df_merged = pd.merge_asof(df_1m.sort_index(), features_5m.sort_index(), left_index=True, right_index=True, direction='backward')
    df_merged = pd.merge_asof(df_merged.sort_index(), features_15m.sort_index(), left_index=True, right_index=True, direction='backward')
    
    df = df_merged.reset_index().ffill().bfill()
    
    # ===== GÁN CÁC CỘT 1M CHÍNH =====
    df['price_return'] = df['price_return_1m']
    df['z_return'] = df['z_return_1m']
    df['z_volume'] = df['z_volume_1m']
    df['close_position'] = df['close_position_1m']
    
    # Z-score khác
    window = 360
    df['z_bid_ask_ratio'] = calculate_rolling_zscore(df['bid_ask_ratio'], window)
    df['z_spread'] = calculate_rolling_zscore(df['spread'], window)
    df['z_top_bid'] = calculate_rolling_zscore(df['top_bid_price'], window)
    
    # Imbalance & spread_pct
    df['order_imbalance'] = (df['top_bid_price'] - df['top_ask_price']) / \
                             (df['top_bid_price'] + df['top_ask_price'] + 1e-8)
    df['spread_pct'] = df['spread'] / (df['close'] + 1e-8)
    df['volume_imbalance'] = df['order_imbalance'] * df['volume']
    
    # ===== ĐIỀU KIỆN PUMP / DUMP NÂNG CẤP ĐA KHUNG THỜI GIAN =====
    cond_1m_return_pump = df['z_return_1m'] > 2.5
    cond_1m_volume_pump = df['z_volume_1m'] > 2.5
    cond_1m_shape_pump = df['close_position_1m'] > 0.7

    cond_1m_return_dump = df['z_return_1m'] < -2.5
    cond_1m_volume_dump = df['z_volume_1m'] > 2.5
    cond_1m_shape_dump = df['close_position_1m'] < 0.3

    cond_5m_return_pump = df['z_return_5m'] > 1.5
    cond_5m_volume_pump = df['z_volume_5m'] > 1.5
    cond_5m_return_dump = df['z_return_5m'] < -1.5
    cond_5m_volume_dump = df['z_volume_5m'] > 1.5

    cond_15m_return_pump = df['z_return_15m'] > 1.0
    cond_15m_volume_pump = df['z_volume_15m'] > 1.0
    cond_15m_return_dump = df['z_return_15m'] < -1.0
    cond_15m_volume_dump = df['z_volume_15m'] > 1.0

    cond_ob_pump = df['z_bid_ask_ratio'] > 1.5
    cond_sentiment_pump = df['final_sentiment_score'] > 0.4
    cond_ob_dump = df['z_bid_ask_ratio'] < -1.5
    cond_sentiment_dump = df['final_sentiment_score'] < -0.4

    pump_conditions = [
        cond_1m_return_pump, cond_1m_volume_pump, cond_1m_shape_pump,
        cond_5m_return_pump, cond_5m_volume_pump,
        cond_15m_return_pump, cond_ob_pump, cond_sentiment_pump
    ]
    dump_conditions = [
        cond_1m_return_dump, cond_1m_volume_dump, cond_1m_shape_dump,
        cond_5m_return_dump, cond_5m_volume_dump,
        cond_15m_return_dump, cond_ob_dump, cond_sentiment_dump
    ]

    pump_sum = sum(cond.astype(int) for cond in pump_conditions)
    dump_sum = sum(cond.astype(int) for cond in dump_conditions)
    
    df['label'] = 0
    df.loc[(pump_sum >= 4) & cond_1m_return_pump & cond_1m_volume_pump, 'label'] = 1
    df.loc[(dump_sum >= 4) & cond_1m_return_dump & cond_1m_volume_dump & (pump_sum < 4), 'label'] = -1
    
    # Future return check
    close_nonzero = df['close'].replace(0, np.nan)
    df['future_return'] = df['close'].shift(-5) / close_nonzero - 1
    has_future = df['future_return'].notna()
    df.loc[(df['label'] == 1) & has_future & (df['future_return'] < 0), 'label'] = 0
    df.loc[(df['label'] == -1) & has_future & (df['future_return'] > 0), 'label'] = 0

    df['pump_conditions'] = pump_sum
    df['dump_conditions'] = dump_sum
    
    return df

# ==============================
# 🧾 CHẠY TRỰC TIẾP
# ==============================
if __name__ == "__main__":
    df = pd.read_csv("/kaggle/input/crypto/final_data.csv")
    if "label" in df.columns:
        df = df.drop(columns=["label"])
    
    df_labeled = label_anomaly_pump_dump(df)
    
    base_cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "top_bid_price", "top_ask_price", "spread",
        "bid_ask_ratio", "final_sentiment_score"
    ]
    
    feature_cols = [
        "price_return", "z_return", "z_volume", "z_bid_ask_ratio",
        "z_spread", "z_top_bid", "close_position",
        'order_imbalance', 'spread_pct', 'volume_imbalance',
        'price_return_5m', 'z_return_5m', 'z_volume_5m', 'close_position_5m',
        'price_return_15m', 'z_return_15m', 'z_volume_15m', 'close_position_15m'
    ]
    
    available_base_cols = [col for col in base_cols if col in df_labeled.columns]
    available_feature_cols = [col for col in feature_cols if col in df_labeled.columns]
    
    df_final = df_labeled[available_base_cols + available_feature_cols + ["label"]]
    
    df_final.to_csv("final_data_labeled.csv", index=False)
    print("✅ Đã lưu final_data_labeled.csv với tất cả features 1m, 5m, 15m và nhãn nâng cấp")
