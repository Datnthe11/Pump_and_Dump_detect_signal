"""
Prepare LSTM/ConvLSTM data với features đa khung thời gian (1m, 5m, 15m)
"""

import pandas as pd
import numpy as np
from collections import Counter
from sklearn.preprocessing import StandardScaler
import joblib
import pickle
import warnings
warnings.filterwarnings("ignore")

# ==========================
# DANH SÁCH FEATURES
# ==========================
BASE_FEATURES = [
    'open', 'high', 'low', 'close', 'volume',
    'top_bid_price', 'top_ask_price', 
    'bid_ask_ratio', 'final_sentiment_score'
]

COMPUTED_FEATURES = [
    'price_return', 'z_return', 'z_volume', 'z_bid_ask_ratio',
    'z_spread', 'z_top_bid', 'close_position',
    'spread_pct', 'volume_imbalance',

    # === THÊM FEATURES MỚI ===
    'price_return_5m', 'z_return_5m', 'z_volume_5m', 'close_position_5m',
    'price_return_15m', 'z_return_15m', 'z_volume_15m', 'close_position_15m'
]

EXPECTED_FEATURES = BASE_FEATURES + COMPUTED_FEATURES

# ==========================
# 1. Tạo sequences (sliding window)
# ==========================
def create_sequences(data, sequence_length=60, features=None, target_col='label'):
    if features is None:
        features = EXPECTED_FEATURES.copy()
    
    missing = [f for f in features if f not in data.columns]
    if missing:
        raise ValueError(f"THIẾU CỘT TRONG DATA: {missing}")
    
    X, y = [], []
    feature_data = data[features].values
    labels = data[target_col].values
    
    for i in range(len(data) - sequence_length + 1):
        seq = feature_data[i:i + sequence_length]
        label = labels[i + sequence_length - 1]
        X.append(seq)
        y.append(label)
    
    return np.array(X), np.array(y), features

# ==========================
# 2. Oversample pump & dump với Augmentation
# ==========================
def oversample_sequences(X, y, factor_pump=8, factor_dump=10, noise_std=0.01, scale_range=(0.95, 1.05), random_state=42, verbose=True):
    np.random.seed(random_state)
    X_res, y_res = [X.copy()], [y.copy()]
    counter_before = Counter(y)

    def augment_batch(X_batch):
        X_aug = X_batch.copy()
        # Add noise
        noise = np.random.normal(0, noise_std, size=X_batch.shape)
        X_aug += noise
        # Scale
        scales = np.random.uniform(scale_range[0], scale_range[1], size=(X_batch.shape[0], 1, 1))
        X_aug *= scales
        # Time warping
        n_timesteps = X_batch.shape[1]
        n_samples = X_batch.shape[0]
        n_control = 4
        control_steps = np.linspace(0, n_timesteps - 1, n_control)
        control_values = np.random.normal(1.0, 0.08, size=(n_samples, n_control))
        timesteps = np.arange(n_timesteps)
        warp_factors = np.array([np.interp(timesteps, control_steps, control_values[i]) for i in range(n_samples)])
        X_aug *= warp_factors[:, :, np.newaxis]
        return X_aug

    for label, factor in [(0, factor_dump), (2, factor_pump)]: # 0=Dump, 2=Pump
        idx = np.where(y == label)[0]
        if len(idx) == 0:
            if verbose:
                print(f"[OVERSAMPLE] Lớp {label}: không có mẫu, bỏ qua")
            continue
        X_minority = X[idx]
        augmented_samples = [augment_batch(X_minority) for _ in range(factor - 1)]
        if augmented_samples:
            X_aug_total = np.concatenate(augmented_samples, axis=0)
            y_aug_total = np.full(X_aug_total.shape[0], label, dtype=y.dtype)
            X_res.append(X_aug_total)
            y_res.append(y_aug_total)

    X_res = np.concatenate(X_res, axis=0)
    y_res = np.concatenate(y_res, axis=0)
    
    # Shuffle
    indices = np.arange(len(y_res))
    np.random.shuffle(indices)
    X_res, y_res = X_res[indices], y_res[indices]

    if verbose:
        counter_after = Counter(y_res)
        print("\n=== Oversampling với Augmentation ===")
        print("Trước oversample:", dict(counter_before))
        print("Sau oversample:", dict(counter_after))

    return X_res, y_res

# ==========================
# 3. Tính class weights
# ==========================
def calculate_class_weights(y):
    from sklearn.utils.class_weight import compute_class_weight
    classes = np.unique(y)
    weights = compute_class_weight('balanced', classes=classes, y=y)
    return {int(cls): float(weight) for cls, weight in zip(classes, weights)}

# ==========================
# 4. Pipeline chính
# ==========================
def main():
    print("=== CHUẨN BỊ DỮ LIỆU LSTM / ConvLSTM (3 CLASS) ===\n")
    df = pd.read_csv("/kaggle/working/final_data_labeled.csv")
    df["open_time"] = pd.to_datetime(df["open_time"])
    df = df.sort_values("open_time").reset_index(drop=True)

    features_to_use = EXPECTED_FEATURES.copy()
    print(f"✅ Sử dụng {len(features_to_use)} features đã tinh gọn.")

    # Xử lý NaN
    df[features_to_use] = df[features_to_use].ffill().bfill().fillna(0)

    # Chuyển label -1→0, 0→1, 1→2
    label_map = {-1:0, 0:1, 1:2}
    df['label'] = df['label'].map(label_map)

    # Chuẩn hóa
    scaler = StandardScaler()
    df[features_to_use] = scaler.fit_transform(df[features_to_use])
    joblib.dump(scaler, "scaler.pkl")
    print("✅ Đã chuẩn hóa features và lưu scaler.pkl")

    # Tạo sequences
    sequence_length = 60
    X, y_raw, feature_names = create_sequences(df, sequence_length=sequence_length, features=features_to_use, target_col='label')
    y = y_raw
    print(f"✅ Đã tạo sequences: {X.shape}")

    # Chia train/val/test
    n = len(X)
    train_end = int(n * 0.8)
    val_end = int(n * 0.9)
    
    X_train_orig, y_train_orig = X[:train_end], y[:train_end]
    X_val, y_val = X[train_end:val_end], y[train_end:val_end]
    X_test, y_test = X[val_end:], y[val_end:]
    print(f"✅ Đã chia Train/Val/Test (80/10/10)")

    # Oversample pump/dump
    X_train, y_train = oversample_sequences(
        X_train_orig, y_train_orig,
        factor_pump=30,
        factor_dump=40,
        noise_std=0.01,
        scale_range=(0.95,1.05),
        random_state=42,
        verbose=True
    )

    # UNDERSAMPLE Normal
    UNDERSAMPLE_RATIO = 20
    anomaly_idx = np.where(y_train != 1)[0]
    n_anomalies = len(anomaly_idx)
    normal_idx = np.where(y_train == 1)[0]
    n_normal_original = len(normal_idx)
    n_keep = min(n_anomalies * UNDERSAMPLE_RATIO, n_normal_original)

    if n_normal_original > n_keep:
        print(f"\n[UNDERSAMPLE] Class 1 (Normal): {n_normal_original:,} → {n_keep:,}")
        np.random.seed(42)
        keep_idx = np.random.choice(normal_idx, n_keep, replace=False)
        final_idx = np.concatenate([keep_idx, anomaly_idx])
        np.random.shuffle(final_idx)
        X_train = X_train[final_idx]
        y_train = y_train[final_idx]
    else:
        print("\n[UNDERSAMPLE] Không cần undersample, số lượng normal đủ ít.")

    print(f"Train class counts (CUỐI CÙNG): {dict(Counter(y_train))}")

    # Lưu dữ liệu
    np.save("X_train.npy", X_train)
    np.save("y_train.npy", y_train)
    np.save("X_train_orig.npy", X_train_orig)
    np.save("y_train_orig.npy", y_train_orig)
    np.save("X_val.npy", X_val)
    np.save("y_val.npy", y_val)
    np.save("X_test.npy", X_test)
    np.save("y_test.npy", y_test)
    print("✅ Đã lưu tất cả các file .npy")

    # Tính class weights
    class_weights_train = calculate_class_weights(y_train)
    class_weights_val = calculate_class_weights(y_val)
    class_weights_test = calculate_class_weights(y_test)

    # Lưu metadata
    metadata = {
        "sequence_length": sequence_length,
        "n_features": len(feature_names),
        "feature_names": feature_names,
        "base_features": BASE_FEATURES,
        "computed_features": [f for f in COMPUTED_FEATURES if f in feature_names],
        "label_mapping": label_map,
        "class_weights": {
            "train": class_weights_train,
            "val": class_weights_val,
            "test": class_weights_test
        },
        "shape": {
            "X_original": X.shape,
            "train": X_train.shape,
            "val": X_val.shape,
            "test": X_test.shape
        },
        "distribution": {
            "train": dict(Counter(y_train)),
            "val": dict(Counter(y_val)),
            "test": dict(Counter(y_test))
        }
    }
    with open("metadata.pkl","wb") as f:
        pickle.dump(metadata,f)
    print("✅ Đã lưu metadata.pkl")
    print("\n=== HOÀN TẤT 100% ===")
    print(f"X_train.shape = {X_train.shape}")
    print(f"Base features: {len(BASE_FEATURES)}, Computed features: {len(COMPUTED_FEATURES)}")


if __name__ == "__main__":
    main()
