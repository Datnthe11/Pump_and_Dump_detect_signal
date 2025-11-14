import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.layers import Layer
from tensorflow.keras.models import load_model

# ========================
# Đăng ký Layer tùy chỉnh
# ========================
class SimpleAttention(Layer):
    def build(self, input_shape):
        features = input_shape[2]
        self.W = self.add_weight(name='W', shape=(features,1), initializer='glorot_uniform', trainable=True)
        self.b = self.add_weight(name='b', shape=(1,), initializer='zeros', trainable=True)

    def call(self, x):
        e = tf.matmul(x, self.W) + self.b
        e = tf.squeeze(tf.nn.tanh(e), axis=-1)
        a = tf.nn.softmax(e, axis=1)
        a = tf.expand_dims(a, axis=-1)
        return tf.reduce_sum(x * a, axis=1)

# ========================
# Load extractor với custom_objects
# ========================
extractor = load_model("lstm_extractor.h5", compile=False, custom_objects={'SimpleAttention': SimpleAttention})
N_LSTM_FEATURES = extractor.output.shape[1]
lstm_cols = [f"lstm_feat_{i}" for i in range(N_LSTM_FEATURES)]

for split in ["train", "val", "test"]:
    print(f"Processing {split}...")
    X_flat = pd.read_parquet(f"X_{split}_flat.parquet")
    X_3d = np.load(f"X_{split}.npy")
    lstm_features = extractor.predict(X_3d, batch_size=512)
    df_lstm = pd.DataFrame(lstm_features, columns=lstm_cols)
    X_hybrid = pd.concat([X_flat.reset_index(drop=True), df_lstm.reset_index(drop=True)], axis=1)
    X_hybrid.to_parquet(f"X_{split}_hybrid.parquet")
    print(f"Saved X_{split}_hybrid.parquet, shape: {X_hybrid.shape}")

print("✅ All hybrid data created.")
