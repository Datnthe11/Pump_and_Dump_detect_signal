import tensorflow as tf
from tensorflow.keras.layers import Layer  # vì bạn dùng SimpleAttention
from sklearn.metrics import classification_report, confusion_matrix
import lightgbm as lgb


extractor = tf.keras.models.load_model(
    "bilstm_attention_extractor.h5",
    custom_objects={'SimpleAttention': SimpleAttention},
    compile=False
)

# Tạo embedding
emb_train = extractor.predict(X_train, batch_size=512)
emb_val   = extractor.predict(X_val, batch_size=512)
emb_test  = extractor.predict(X_test, batch_size=512)

import lightgbm as lgb

lgbm = lgb.LGBMClassifier(
    n_estimators=2000,
    max_depth=-1,
    learning_rate=0.01,
    subsample=0.8,
    colsample_bytree=0.8,
    objective='multiclass',
    num_class=3
)

lgbm.fit(
    emb_train, y_train_label,
    eval_set=[(emb_val, y_val_label)],
    verbose=100
)





# Dự đoán
pred_test = lgbm.predict(emb_test)

# Confusion matrix
cm = confusion_matrix(y_test_label, pred_test)
print("Confusion Matrix:\n", cm)

# Classification report
cr = classification_report(y_test_label, pred_test, target_names=["Dump", "Normal", "Pump"])
print("Classification Report:\n", cr)
