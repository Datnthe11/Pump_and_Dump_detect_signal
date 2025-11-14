import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import json

# ======= Cấu hình =======
API_KEY = " "  # Thay bằng API key thật
BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"

HEADERS = {"X-API-Key": API_KEY}

# Keywords về crypto - Loại bỏ từ nhiễu cao
KEYWORDS = (
    # Các coin chính (BẮT BUỘC phải có ít nhất 1 trong số này)
    '('
    '$BTC OR $ETH OR $SOL OR $BNB OR $ADA OR $DOGE OR $XRP '
    'OR #Bitcoin OR #Ethereum OR #Solana OR #Crypto OR #Cryptocurrency '
    'OR bitcoin OR ethereum OR solana OR "crypto" '
    
    # Thuật ngữ về scam/manipulation (CHỈ dùng cụm từ, KHÔNG dùng từ đơn)
    'OR rugpull OR "rug pull" OR "pump and dump" OR "exit scam" '
    'OR honeypot OR "crypto scam" OR "market manipulation" '
    
    # Tín hiệu giao dịch (CHỈ dùng cụm từ)
    'OR "buy signal" OR "sell signal" OR "buy now" OR "sell now" '
    'OR "massive buy" OR "massive sell" OR "whale alert" OR "whale movement" '
    'OR "buying opportunity" OR "selling pressure" '
    
    # Thuật ngữ crypto phổ biến (CHỈ dùng cụm từ hoặc từ viết hoa đặc biệt)
    'OR "to the moon" OR "moon soon" OR rekt OR REKT '
    'OR "bull run" OR "bear trap" OR "bull trap" OR "bear market" OR "bull market" '
    'OR FOMO OR HODL OR "buying the dip" OR "diamond hands" OR "paper hands" '
    'OR "100x" OR "10x gem" OR "moon shot" OR "gem alert" '
    'OR "altcoin season" OR "alt season" OR "meme season" '
    'OR shitcoin OR memecoin OR "pump incoming" OR "shilling" '
    'OR "crypto twitter" OR "crypto news" OR "degen" OR "ape in" '
    ')'
)

QUERY_TYPE = "Latest"
TWEETS_PER_DAY = 100  # Mục tiêu tweets mỗi ngày
OUTPUT_CSV = "tweets_2025Q3_crypto.csv"
OUTPUT_JSON = "tweets_2025Q3_crypto.json"

# Debug mode - Bật để xem tweets API trả về
DEBUG_MODE = True  # Đặt False sau khi test xong

# ⚠️ Thời gian crawl - Q3/2025 (1/7 - 30/9/2025)
START_DATE = datetime(2025, 7, 1)
END_DATE = datetime(2025, 9, 30)

# ======= Hàm fetch page =======
def fetch_page(query_string, cursor=None):
    """
    Fetch một trang tweets từ API
    API tự động trả max 20 tweets/page
    """
    params = {
        "query": query_string,  # since/until phải nằm TRONG query string
        "queryType": QUERY_TYPE
    }
    
    if cursor:
        params["cursor"] = cursor

    try:
        resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
        
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            print(f"⚠️ Rate limit! Chờ 60s...")
            time.sleep(60)
            return fetch_page(query_string, cursor)  # Retry
        elif resp.status_code == 401:
            print(f"❌ API key không hợp lệ!")
            return None
        else:
            print(f"❌ API lỗi: {resp.status_code} - {resp.text}")
            return None
            
    except requests.RequestException as e:
        print(f"❌ Lỗi kết nối: {e}")
        return None

# ======= Crawl tweet cho 1 ngày =======
def fetch_tweets_for_day(day):
    """
    Lấy tweets cho một ngày cụ thể
    Sử dụng since/until TRONG query string theo format Twitter API
    """
    all_tweets = []
    seen_ids = set()
    cursor = None
    page = 0
    
    # Format ngày: YYYY-MM-DD (format ngắn gọn - đã test work!)
    date_str = day.strftime("%Y-%m-%d")
    next_day_str = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # ⚠️ GỌN HƠN: Chỉ dùng coin symbols + since/until
    # Query phức tạp làm API bỏ qua filter thời gian!
    SIMPLE_CRYPTO = '($BTC OR $ETH OR $SOL OR #Bitcoin OR #Ethereum OR #Crypto)'
    
    # ✅ Query đơn giản: coin + time + language
    query_string = (
        f'{SIMPLE_CRYPTO} '
        f'lang:en -is:retweet -is:quote '
        f'since:{date_str} until:{next_day_str}'
    )
    
    print(f"\n📅 Đang crawl ngày {date_str}...")
    print(f"📝 Query: {query_string}")  # In TOÀN BỘ query để debug
    print(f"📏 Query length: {len(query_string)} chars")
    
    while len(all_tweets) < TWEETS_PER_DAY:
        page += 1
        print(f"  📄 Page {page}...", end=" ")
        
        data = fetch_page(query_string, cursor)
        
        if data is None:
            print("❌ Lỗi API")
            break
        
        tweets = data.get("tweets", [])
        has_next = data.get("has_next_page", False)
        next_cursor = data.get("next_cursor")
        
        if not tweets:
            print(f"✓ Không còn tweets (total: {len(all_tweets)})")
            break
        
        # DEBUG: In ra thời gian của tweets
        if DEBUG_MODE and page == 1:
            print(f"\n  🔍 DEBUG - API trả về {len(tweets)} tweets:")
            for i, t in enumerate(tweets[:3], 1):
                created = t.get("createdAt", "N/A")
                text_preview = t.get("text", "")[:50]
                print(f"     {i}. {created}")
                print(f"        \"{text_preview}...\"")
            print(f"  ", end="")
        
        # Lọc và thêm tweets
        new_count = 0
        skipped_count = 0
        for t in tweets:
            tid = t.get("id")
            if tid and tid not in seen_ids:
                # Kiểm tra thời gian (nhưng KHÔNG skip nếu API đã filter)
                tweet_date_ok = True
                try:
                    created_at_str = t.get("createdAt", "")
                    if created_at_str:
                        created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
                        created_at_utc = created_at.astimezone(None).replace(tzinfo=None)
                        
                        # So sánh ngày (cho phép sai lệch 1 ngày do timezone)
                        date_diff = abs((created_at_utc.date() - day.date()).days)
                        if date_diff > 1:
                            tweet_date_ok = False
                            skipped_count += 1
                except Exception as e:
                    # Nếu parse lỗi, vẫn lấy (tin tưởng API filter)
                    pass
                
                if tweet_date_ok:
                    seen_ids.add(tid)
                    all_tweets.append(t)
                    new_count += 1
                    
                    if len(all_tweets) >= TWEETS_PER_DAY:
                        break
        
        if skipped_count > 0:
            print(f"(skipped {skipped_count} wrong date)", end=" ")
        
        print(f"✓ +{new_count} tweets (total: {len(all_tweets)})")
        
        # Kiểm tra có trang tiếp theo không
        if not has_next or not next_cursor or len(all_tweets) >= TWEETS_PER_DAY:
            break
            
        cursor = next_cursor
        time.sleep(0.5)  # Tránh rate limit
    
    print(f"✅ Ngày {date_str}: Lấy được {len(all_tweets)} tweets")
    return all_tweets

# ======= Lưu CSV =======
def save_csv(tweets, file_path):
    """Lưu tweets vào CSV (append mode)"""
    if not tweets:
        return
    
    df = pd.json_normalize(tweets)
    
    # Kiểm tra file đã tồn tại chưa
    file_exists = os.path.exists(file_path)
    
    # Append nếu file đã có, create mới nếu chưa
    df.to_csv(
        file_path,
        index=False,
        mode='a' if file_exists else 'w',
        header=not file_exists,
        encoding='utf-8'
    )
    
    print(f"💾 Đã lưu {len(tweets)} tweets vào {file_path}")

# ======= Lưu JSON =======
def save_json(tweets, file_path):
    """Lưu tweets vào JSON (merge với data cũ)"""
    if not tweets:
        return
    
    existing = []
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing = json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ File JSON bị lỗi, sẽ tạo mới")
    
    all_data = existing + tweets
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Đã lưu tổng {len(all_data)} tweets vào {file_path}")

# ======= Hàm kiểm tra API key =======
def check_api_key():
    """Kiểm tra API key có hợp lệ không"""
    if not API_KEY or API_KEY == "":
        print("❌ CẢNH BÁO: API_KEY trống! Vui lòng thêm API key.")
        return False
    
    # Test API key với query đơn giản
    test_query = "$BTC lang:en"
    data = fetch_page(test_query)
    
    if data is None:
        print("❌ API key không hợp lệ hoặc không có quyền truy cập")
        return False
    
    print("✅ API key hợp lệ!")
    return True

# ======= Main =======
def main():
    print("=" * 70)
    print("🚀 TWITTER CRYPTO CRAWLER - Q3/2025 (1/7 - 30/9/2025)")
    print("=" * 70)
    
    # Kiểm tra API key
    if not check_api_key():
        return
    
    # Thông báo thời gian crawl
    print(f"\n📆 Thời gian crawl:")
    print(f"   Từ: {START_DATE.strftime('%d/%m/%Y')}")
    print(f"   Đến: {END_DATE.strftime('%d/%m/%Y')}")
    print(f"   Tổng: {(END_DATE - START_DATE).days + 1} ngày")
    print(f"   Mục tiêu: ~{TWEETS_PER_DAY} tweets/ngày")
    
    # Xác nhận
    print(f"\n⚠️  Lưu ý: Dữ liệu sẽ được lưu vào:")
    print(f"   - {OUTPUT_CSV}")
    print(f"   - {OUTPUT_JSON}")
    
    input("\n👉 Nhấn Enter để bắt đầu crawl...")
    
    # Bắt đầu crawl
    current_day = START_DATE
    total_tweets = 0
    success_days = 0
    
    while current_day <= END_DATE:
        tweets = fetch_tweets_for_day(current_day)
        
        if tweets:
            save_csv(tweets, OUTPUT_CSV)
            save_json(tweets, OUTPUT_JSON)
            total_tweets += len(tweets)
            success_days += 1
        
        current_day += timedelta(days=1)
        
        # Nghỉ giữa các ngày
        if current_day <= END_DATE:
            print(f"⏸️  Chờ 2s trước khi crawl ngày tiếp theo...")
            time.sleep(2)
    
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH!")
    print("=" * 70)
    print(f"✅ Crawl thành công: {success_days}/{(END_DATE - START_DATE).days + 1} ngày")
    print(f"📊 Tổng số tweets: {total_tweets}")
    print(f"📈 Trung bình: {total_tweets // success_days if success_days > 0 else 0} tweets/ngày")
    print(f"📁 File CSV: {OUTPUT_CSV}")
    print(f"📁 File JSON: {OUTPUT_JSON}")
    print("=" * 70)

if __name__ == "__main__":
    main()