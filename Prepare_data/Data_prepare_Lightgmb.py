# -*- coding: utf-8 -*-
"""
Chuẩn bị dữ liệu LightGBM từ sequences 3D (X_train, X_val, X_test)
Tính toán thống kê mean/std/max/min cho các feature quan trọng, bao gồm cả 1m, 5m, 15m
"""

import numpy as np
import pandas as pd
from tqdm import tqdm
import pickle

# ==========================
# 1. LOAD METADATA
# ==========================
with open("metadata.pkl", "rb") as f:
    metadata = pickle.load(f)

FEATURE_NAMES = metadata['feature_names']  # 18 features gốc + 8 tính từ 5m/15m
SEQUENCE_LENGTH = metadata['sequence_length']

print(f"Sẽ làm phẳng {len(FEATURE_NAMES)} features trên {SEQUENCE_LENGTH} timesteps.")

# ==========================
# 2. HÀM FLATTEN SEQUENCES
# ==========================
def flatten_sequences(X_3d, feature_names):
    """
    Biến đổi dữ liệu 3D (samples, 60, n_features) 
    thành 2D (samples, n_features_mới) với thống kê mean/std/max/min
    """
    n_samples = X_3d.shape[0]

    # Danh sách các DataFrame tạm để ghép
    new_features_list = []

    # 1️⃣ Features ở timestep cuối cùng (quan trọng nhất)
    last_step_data = X_3d[:, -1, :]
    last_step_cols = [f"{col}_last" for col in feature_names]
    df_last_step = pd.DataFrame(last_step_data, columns=last_step_cols)
    new_features_list.append(df_last_step)

    # 2️⃣ Features thống kê: mean, std, max, min
    # --- Thêm cả các features mới 5m/15m ---
    important_cols = [
        'z_return', 'z_volume', 'z_spread', 'z_bid_ask_ratio',
        'price_return', 'close_position', 'spread_pct',
        # Features mới 5m
        'z_return_5m', 'z_volume_5m', 'close_position_5m',
        # Features mới 15m
        'z_return_15m', 'z_volume_15m', 'close_position_15m'
    ]

    for col_name in tqdm(important_cols, desc="Tạo features thống kê"):
        if col_name not in feature_names:
            continue
        
        col_idx = feature_names.index(col_name)
        col_data_3d = X_3d[:, :, col_idx]  # (n_samples, sequence_length)

        # Thống kê 60 timesteps
        df_stat = pd.DataFrame({
            f"{col_name}_mean_60": np.mean(col_data_3d, axis=1),
            f"{col_name}_std_60": np.std(col_data_3d, axis=1),
            f"{col_name}_max_60": np.max(col_data_3d, axis=1),
            f"{col_name}_min_60": np.min(col_data_3d, axis=1),
        })
        new_features_list.append(df_stat)

        # Thống kê 10 timesteps gần nhất
        col_data_last10 = X_3d[:, -10:, col_idx]
        df_stat_10 = pd.DataFrame({
            f"{col_name}_mean_10": np.mean(col_data_last10, axis=1),
            f"{col_name}_max_10": np.max(col_data_last10, axis=1),
        })
        new_features_list.append(df_stat_10)

    # Ghép tất cả DataFrame lại
    X_flat = pd.concat(new_features_list, axis=1)
    return X_flat

# ==========================
# 3. CHẠY PIPELINE
# ==========================
print("\nĐang load dữ liệu 3D (X_train, X_val, X_test)...")
X_train_3d = np.load("X_train.npy")
y_train = np.load("y_train.npy")
X_val_3d = np.load("X_val.npy")
y_val = np.load("y_val.npy")
X_test_3d = np.load("X_test.npy")
y_test = np.load("y_test.npy")

print("\nBắt đầu làm phẳng X_train...")
X_train_flat = flatten_sequences(X_train_3d, FEATURE_NAMES)
print("Bắt đầu làm phẳng X_val...")
X_val_flat = flatten_sequences(X_val_3d, FEATURE_NAMES)
print("Bắt đầu làm phẳng X_test...")
X_test_flat = flatten_sequences(X_test_3d, FEATURE_NAMES)

print(f"\nĐã làm phẳng! Shape Train: {X_train_flat.shape}")

# ==========================
# 4. LƯU DỮ LIỆU 2D CHO LIGHTGBM
# ==========================
print("Đang lưu dữ liệu 2D...")
X_train_flat.to_parquet("X_train_flat.parquet")
X_val_flat.to_parquet("X_val_flat.parquet")
X_test_flat.to_parquet("X_test_flat.parquet")

# Lưu nhãn
np.save("y_train_lgbm.npy", y_train)
np.save("y_val_lgbm.npy", y_val)
np.save("y_test_lgbm.npy", y_test)

print("=== HOÀN TẤT CHUẨN BỊ DATA CHO LIGHTGBM ===")
