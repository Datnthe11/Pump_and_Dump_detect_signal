import tensorflow as tf
from tensorflow.keras.layers import Layer, Input, Bidirectional, LSTM, Dense, Dropout, BatchNormalization
from tensorflow.keras.models import Model

# =========================
# Attention Layer
# =========================
class SimpleAttention(Layer):
    def build(self, input_shape):
        features = input_shape[2]
        self.W = self.add_weight(name='W', shape=(features,1),
                                 initializer='glorot_uniform', trainable=True)
        self.b = self.add_weight(name='b', shape=(1,),
                                 initializer='zeros', trainable=True)
    def call(self, x):
        e = tf.matmul(x, self.W) + self.b
        e = tf.squeeze(tf.nn.tanh(e), axis=-1)
        a = tf.nn.softmax(e, axis=1)
        a = tf.expand_dims(a, axis=-1)
        return tf.reduce_sum(x * a, axis=1)  # (batch, features)
# =========================
# BiLSTM + Attention Extractor
# =========================
def build_extractor(seq_len=60, n_features=18, embedding_dim=32, lstm_units=[64,32], dropout=0.3):
    inp = Input(shape=(seq_len, n_features), name="seq_input")
    x = inp
    for i, units in enumerate(lstm_units):
        x = Bidirectional(LSTM(units, return_sequences=True, name=f"bilstm_{i+1}"))(x)
        x = BatchNormalization()(x)
        x = Dropout(dropout)(x)
    att = SimpleAttention()(x)
    out = Dense(embedding_dim, activation='linear', name="embedding")(att)
    model = Model(inputs=inp, outputs=out, name="BiLSTM_Attention_Extractor")
    return model
extractor = build_extractor(seq_len=60, n_features=18, embedding_dim=32)
extractor.compile(optimizer='adam', loss='mse')

# Self-supervised: predict last timestep
y_train = X_train[:, -1, :]
y_val   = X_val[:, -1, :]

extractor.fit(
    X_train, y_train,
    validation_data=(X_val, y_val),
    epochs=10,
    batch_size=512
)

extractor.save("bilstm_attention_extractor.h5")
