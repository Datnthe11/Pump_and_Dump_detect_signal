import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm
import json
import re
import emoji

# ==========================
# ⚙️ Cấu hình file
# ==========================
INPUT_FILE = "tweets_two_fields.csv"   # chứa cột created_at, content
OUTPUT_CSV = "tweets_sentiment_roberta.csv"
OUTPUT_JSONL = "tweets_sentiment_roberta.jsonl"

# ==========================
# 🚀 Load model & tokenizer
# ==========================
MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)
model.eval()

# ==========================
# 🧹 Hàm chuẩn hoá tweet
# ==========================
def clean_tweet(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r"http\S+|www\S+", "", text)  # Bỏ URL
    text = re.sub(r"@\w+", "@user", text)       # Thay mention bằng @user

    # Chuẩn hoá hashtag -> tách từ
    def split_hashtag(tag):
        tag = tag.group()[1:]
        return " ".join(re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)", tag))
    text = re.sub(r"#\w+", split_hashtag, text)

    # Chuyển emoji → text mô tả
    text = emoji.demojize(text, delimiters=(" ", " "))
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ==========================
# 🧠 Hàm tính sentiment (RoBERTa)
# ==========================
def roberta_sentiment(texts):
    inputs = tokenizer(
        texts,
        return_tensors="pt",
        truncation=True,
        max_length=512,
        padding=True
    )
    if "token_type_ids" in inputs:
        del inputs["token_type_ids"]

    with torch.no_grad():
        outputs = model(**inputs)
        scores = torch.nn.functional.softmax(outputs.logits, dim=-1)

    results = []
    for s in scores:
        results.append({
            "neg": float(s[0]),
            "neu": float(s[1]),
            "pos": float(s[2])
        })
    return results

# ==========================
# 📥 Đọc dữ liệu
# ==========================
df = pd.read_csv(INPUT_FILE)
df = df[["created_at", "content"]].dropna()
print(f"Loaded {len(df)} tweets from {INPUT_FILE}")

# ==========================
# ⚡ Batch xử lý nhanh
# ==========================
batch_size = 32
results = []

for i in tqdm(range(0, len(df), batch_size), desc="Scoring sentiment"):
    batch = df.iloc[i:i+batch_size]
    cleaned_texts = [clean_tweet(t) for t in batch["content"].tolist()]
    sentiments = roberta_sentiment(cleaned_texts)

    for j, s in enumerate(sentiments):
        results.append({
            "created_at": batch.iloc[j]["created_at"],
            "content": cleaned_texts[j],
            "neg": s["neg"],
            "neu": s["neu"],
            "pos": s["pos"]
        })

# ==========================
# 💾 Lưu kết quả
# ==========================
out_df = pd.DataFrame(results)
out_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
    for item in results:
        json.dump(item, f, ensure_ascii=False)
        f.write("\n")

print(f"\n✅ Saved sentiment results to:\n- {OUTPUT_CSV}\n- {OUTPUT_JSONL}")
