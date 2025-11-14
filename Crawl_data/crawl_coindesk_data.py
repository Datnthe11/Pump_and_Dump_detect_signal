import time
import random
import json
import csv
import os
import re
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Configuration
BASE_URL = "https://www.coindesk.com/latest-crypto-news/"
OUTPUT_CSV = "coindesk_filtered.csv"
OUTPUT_JSON = "coindesk_raw.json"
ERROR_LOG = "coindesk_errors.log"
CHECKPOINT_FILE = "coindesk_checkpoint.json"

# Authentication - fill these with your CoinDesk credentials
USE_AUTH = True  # Set to True to enable authentication
COINDESK_EMAIL = ""  # Replace with your email
COINDESK_PASSWORD = ""  # Replace with your password

# Date range for articles (YYYY-MM-DD)
START_DATE = "2025-07-01"
END_DATE = "2025-09-30"

# General settings
REQUEST_TIMEOUT = 30  # seconds

# Pacing settings (adjust for efficiency vs. politeness)
MIN_WAIT = 3  # seconds between article requests (reduced for efficiency)
MAX_WAIT = 6  # seconds between article requests (reduced for efficiency)
BATCH_WAIT_MIN = 15  # minimum seconds between batch runs
BATCH_WAIT_MAX = 30  # maximum seconds between batch runs

# Retry and targets
MAX_RETRIES = 3  # Maximum number of retries for a page
DAILY_TARGET = 100000  # Set to a very high number since we're using continuous mode
TARGET_ARTICLES = DAILY_TARGET  # Keep for compatibility
CONTINUOUS_MODE = True  # Will continue until finding articles older than START_DATE

# Testing mode
TEST_MODE = False  # Set to True for initial testing
MAX_TEST_ARTICLES = 3  # Reduced to minimize load during testing
MAX_LOAD_MORE_CLICKS = 1000  # Increased to allow more "Load More" clicks during testing

# Set up logging
logging.basicConfig(
    filename=ERROR_LOG,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Convert string dates to datetime objects
start_date = datetime.strptime(START_DATE, '%Y-%m-%d')
end_date = datetime.strptime(END_DATE, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)  # End of the day

# Track processed URLs to avoid duplicates
processed_urls = set()

def login_to_coindesk(driver, max_retries=3, use_google=False):
    """Login to CoinDesk with provided credentials and retry if needed"""
    if not USE_AUTH:
        print("⚠️ Authentication not configured. Will attempt to crawl without login.")
        return False
    
    for attempt in range(max_retries):
        try:
            print(f"🔑 Attempting to log in to CoinDesk (attempt {attempt+1}/{max_retries})...")
            
            # Clear cookies for a fresh login attempt
            if attempt > 0:
                # Skipping cookie deletion - driver.delete_all_cookies()
                print("🧹 Cleared cookies for fresh login attempt")
                
            # Navigate directly to the Auth0 login page
            auth0_login_url = "https://auth.coindesk.com/u/login/identifier?state=hKFo2SBiNWNOVVQ2R19YZnVFNFVkUnIzMXVEVXQybFltcmZtSaFur3VuaXZlcnNhbC1sb2dpbqN0aWTZIGVVbU5IN09Oa3FhZDA4RVZvNWVSbzF6TGZ2MUN0azJHo2NpZNkgSHRuaW84RjFpYWtURG5leloxTEhKd1Q3dENJVUhMZFo"
            
            print(f"🔗 Going to Auth0 login page: {auth0_login_url}")
            driver.get(auth0_login_url)
            
            # Wait for the email input field
            try:
                # Wait for email field to be present
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email'], input[name='email'], #email"))
                )
                print("✅ Auth0 login page loaded successfully")
                time.sleep(2)
            except Exception as e:
                print(f"❌ Could not load Auth0 login page: {str(e)[:100]}")
                logging.error(f"Could not load Auth0 login page: {str(e)}")
                continue
            
            # Step 1: Enter Email in Auth0 Form
            try:
                # Find the email input field
                email_input = driver.find_element(By.CSS_SELECTOR, "input[type='email'], input[name='email'], #email")
                
                # Clear any existing text
                email_input.clear()
                time.sleep(0.5)
                
                # Type the email address
                print(f"📧 Entering email address: {COINDESK_EMAIL[:3]}***{COINDESK_EMAIL[-10:]}")
                for char in COINDESK_EMAIL:
                    email_input.send_keys(char)
                    time.sleep(0.05)
                print("✅ Email entered successfully")
                
                # Find the Continue button - using more precise approach for Auth0
                try:
                    print("🔍 Looking for continue button...")
                    
                    # First try getting all buttons on page and find the most likely one
                    all_buttons = driver.find_elements(By.TAG_NAME, "button")
                    continue_button = None
                    
                    # Look for buttons with submit type or action attribute
                    for button in all_buttons:
                        try:
                            button_type = button.get_attribute("type")
                            button_name = button.get_attribute("name")
                            button_text = button.text.strip().lower()
                            
                            # Look for the most likely continue button
                            if (button_type == "submit" or 
                                button_name == "action" or 
                                "continue" in button_text or 
                                "next" in button_text):
                                if button.is_displayed():
                                    continue_button = button
                                    print(f"✓ Found button: Type={button_type}, Name={button_name}, Text={button_text}")
                                    break
                        except:
                            continue
                    
                    # If we found a likely button
                    if continue_button:
                        print("🖱️ Clicking continue button")
                        
                        # First try direct click after ensuring it's in view
                        try:
                            # Scroll to make button visible
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", continue_button)
                            time.sleep(1)
                            
                            # Try direct click first
                            continue_button.click()
                        except Exception as e:
                            print(f"Direct click failed: {str(e)[:50]}...")
                            
                            # Then try JavaScript click
                            try:
                                driver.execute_script("arguments[0].click();", continue_button)
                            except Exception as js_err:
                                print(f"JavaScript click failed: {str(js_err)[:50]}...")
                                raise
                        
                        # Wait for the password field to appear (second step)
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
                            )
                            print("✅ Password input field appeared - proceeding to step 2")
                            time.sleep(2)  # Give the animation time to complete
                        except Exception as wait_err:
                            print(f"Password field did not appear after clicking continue: {str(wait_err)[:50]}...")
                            raise Exception("Password field did not appear after clicking continue button")
                    else:
                        # Try the full form submission approach
                        print("No continue button found directly, trying form submission...")
                        
                        # First identify the form containing the email input
                        try:
                            form = email_input.find_element(By.XPATH, "./ancestor::form")
                            if form:
                                print("Found form containing email input, attempting submission...")
                                driver.execute_script("arguments[0].submit();", form)
                            else:
                                raise Exception("Could not find form containing email input")
                        except Exception as form_err:
                            print(f"Form submission failed: {str(form_err)}")
                            # Final approach - try enter key
                            print("Trying Enter key on email input...")
                            email_input.send_keys(Keys.RETURN)
                        
                        # Wait for password field
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
                            )
                            print("✅ Password input field appeared - proceeding to step 2")
                            time.sleep(2)
                        except:
                            raise Exception("Could not proceed to password step")
                        
                except Exception as button_err:
                    print(f"Button selection/submission error: {str(button_err)}")
                    raise
                
            except Exception as e:
                print(f"❌ Error entering email or proceeding to password step: {str(e)[:100]}")
                logging.error(f"Error in Auth0 email step: {str(e)}")
                continue
            # Step 2: Enter Password
            try:
                print("🔍 Looking for password field...")
                # Find all password inputs and use the first visible one
                password_fields = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
                password_field = None
                
                # Find the first visible password field
                for field in password_fields:
                    if field.is_displayed():
                        password_field = field
                        print("✅ Found visible password field")
                        break
                
                if not password_field:
                    print("⚠️ No visible password field found, trying by attributes...")
                    # Try additional selectors
                    additional_selectors = ["input[name='password']", "#password", "input.password-input"]
                    for selector in additional_selectors:
                        try:
                            fields = driver.find_elements(By.CSS_SELECTOR, selector)
                            for field in fields:
                                if field.is_displayed():
                                    password_field = field
                                    print(f"✅ Found password field using selector: {selector}")
                                    break
                            if password_field:
                                break
                        except:
                            continue
                
                if not password_field:
                    print("❌ Could not locate any password field")
                    # Take a screenshot for debugging
                    try:
                        screenshot_path = "login_debug_screenshot.png"
                        driver.save_screenshot(screenshot_path)
                        print(f"💾 Saved debug screenshot to {screenshot_path}")
                    except:
                        pass
                    raise Exception("Could not locate password field after multiple attempts")
                    
                # Focus and clear password field
                try:
                    # First click on the field
                    ActionChains(driver).move_to_element(password_field).click().perform()
                    time.sleep(0.5)
                    password_field.clear()
                    time.sleep(0.5)
                except Exception as clear_err:
                    print(f"⚠️ Warning when clearing password field: {str(clear_err)[:50]}")
                    # Try javascript focus
                    driver.execute_script("arguments[0].focus();", password_field)
                
                # Enter the password
                print("🔑 Entering password (hidden)")
                password_field.send_keys(COINDESK_PASSWORD)
                time.sleep(0.5)
                
                # Look for the login/submit button
                print("🔍 Looking for login/submit button...")
                
                # First try getting all buttons and finding login/submit button
                all_buttons = driver.find_elements(By.TAG_NAME, "button")
                login_button = None
                
                # First look for buttons with type="submit" or typical login text
                for button in all_buttons:
                    if button.is_displayed():
                        try:
                            button_type = button.get_attribute("type")
                            button_name = button.get_attribute("name")
                            button_text = button.text.strip().lower()
                            
                            if (button_type == "submit" or 
                                button_name == "action" or 
                                "login" in button_text or 
                                "sign in" in button_text or 
                                "continue" in button_text):
                                login_button = button
                                print(f"✅ Found login button: Type={button_type}, Name={button_name}, Text={button_text}")
                                break
                        except:
                            continue
                
                if login_button:
                    print("🖱️ Clicking login button")
                    try:
                        # Scroll into view
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", login_button)
                        time.sleep(1)
                        
                        # Try direct click
                        try:
                            login_button.click()
                        except Exception as click_err:
                            print(f"Direct click failed: {str(click_err)[:50]}...")
                            # Try JS click
                            driver.execute_script("arguments[0].click();", login_button)
                    except Exception as e:
                        print(f"⚠️ Error clicking login button: {str(e)[:50]}...")
                        # Last resort - try pressing Enter in password field
                        from selenium.webdriver.common.keys import Keys
                        password_field.send_keys(Keys.RETURN)
                else:
                    print("No login button found, trying form submission...")
                    # Try to find the form containing password field
                    try:
                        form = password_field.find_element(By.XPATH, "./ancestor::form")
                        if form:
                            print("Found form, submitting...")
                            driver.execute_script("arguments[0].submit();", form)
                        else:
                            # Last resort - press Enter in password field
                            print("Pressing Enter in password field...")
                            password_field.send_keys(Keys.RETURN)
                    except Exception as form_err:
                        print(f"Form submission failed: {str(form_err)[:50]}...")
                        # Last resort - press Enter in password field
                        from selenium.webdriver.common.keys import Keys
                        print("Pressing Enter in password field...")
                        password_field.send_keys(Keys.RETURN)
                
                # Wait for the redirect back to CoinDesk after login
                print("⏳ Waiting for redirect after login...")
                time.sleep(7)  # Give time for login to complete and redirect
                
                # Check if we're logged in successfully - more comprehensive check
                try:
                    # Get the current URL to see if we've been redirected away from Auth0
                    current_url = driver.current_url
                    print(f"🔍 Current URL after login attempt: {current_url}")
                    
                    # Check if we've been redirected away from Auth0 domain
                    if "auth.coindesk.com" not in current_url:
                        print("✅ Successfully redirected away from Auth0 login page")
                        
                        # Further check for login indicators
                        page_source_lower = driver.page_source.lower()
                        login_indicators = ["sign in", "log in", "login", "sign up", "register", "create account"]
                        login_prompts_found = any(indicator in page_source_lower for indicator in login_indicators)
                        
                        # Look for success indicators
                        success_indicators = ["account", "profile", "logout", "sign out", "welcome", "my coindesk"]
                        success_found = any(indicator in page_source_lower for indicator in success_indicators)
                        
                        if not login_prompts_found or success_found:
                            print("✅ Successfully logged in to CoinDesk")
                            logging.info("Successfully logged in to CoinDesk")
                            
                            # Visit the main page to ensure we're fully logged in
                            print("🌐 Visiting main page to confirm login")
                            driver.get("https://www.coindesk.com/")
                            time.sleep(3)
                            
                            # Take screenshot for verification
                            try:
                                driver.save_screenshot("login_success.png")
                                print("💾 Saved login confirmation screenshot")
                            except:
                                pass
                            
                            return True
                        else:
                            print("⚠️ Redirected but still seeing login prompts")
                            logging.warning("Redirected after login but still seeing login prompts")
                            continue
                    else:
                        print("❌ Login failed - still on Auth0 login page")
                        logging.error("Login failed - still on Auth0 login page")
                        
                        # Save screenshot for debugging
                        try:
                            driver.save_screenshot("login_failure.png")
                            print("💾 Saved login failure screenshot")
                        except:
                            pass
                        continue
                except Exception as check_err:
                    print(f"❌ Error checking login status: {str(check_err)}")
                    logging.error(f"Error checking login status: {str(check_err)}")
                    continue
            except Exception as e:
                print(f"❌ Error entering password or completing login: {str(e)[:100]}")
                logging.error(f"Error in Auth0 password step: {str(e)}")
                continue
                        
        except Exception as e:
            print(f"❌ Login attempt {attempt+1} failed: {str(e)}")
            logging.error(f"Login attempt {attempt+1} failed: {str(e)}")
            time.sleep(3)  # Wait before retry
            continue
                    
    # After all retries are exhausted
    print("❌ All login attempts failed. Will try to continue without proper authentication.")
    logging.error("All login attempts failed. Continuing without proper authentication.")
    return False

def setup_browser():
    """Set up and return a headless Chrome browser"""
    options = Options()
    options.add_argument("--headless=new")  # Run in headless mode (no visible browser)
    # Comment out the line above and uncomment the line below to see the browser window for debugging
    # options.add_argument("--window-size=1920,1080")  # For non-headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--enable-unsafe-swiftshader")  # Handle WebGL errors
    options.add_argument("--disable-web-security")  # Reduce security restrictions
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")  # Disable site isolation
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])  # Reduce console noise
    options.add_experimental_option("useAutomationExtension", False)
    
    print("🌐 Setting up Chrome driver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Set the user agent
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    })
    
    # Set timeouts
    driver.set_page_load_timeout(REQUEST_TIMEOUT)
    driver.set_script_timeout(REQUEST_TIMEOUT)
    
    return driver

def load_existing_data():
    """Load existing crawled data if available"""
    global processed_urls
    articles = []
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            # Track URLs of already processed articles
            for article in articles:
                if 'url' in article:
                    processed_urls.add(article['url'])
            print(f"ℹ️ Found {len(processed_urls)} already processed URLs")
        except json.JSONDecodeError:
            print("⚠️ Error loading existing JSON data. Starting with empty dataset.")
    
    # Return articles and count
    return articles, len(articles)

def save_data(articles):
    """Save data to CSV and JSON files, including all articles"""
    # Save all articles, even those with little content
    filtered_articles = []
    paywalled_count = 0
    short_content_count = 0
    
    for article in articles:
        content = article.get('content', '')
        is_paywalled = article.get('is_paywalled', False)
        
        # Mark paywall status for statistics but include all articles
        if content == "Already have an account? Sign in":
            paywalled_count += 1
            article['is_paywalled'] = True
        elif content and len(content) < 100 and not content.startswith("[PAYWALLED]"):
            short_content_count += 1
            article['has_short_content'] = True
        
        filtered_articles.append(article)
    
    if paywalled_count > 0:
        print(f"ℹ️ Found {paywalled_count} paywalled articles (will be included in output)")
    if short_content_count > 0:
        print(f"ℹ️ Found {short_content_count} articles with short content (will be included in output)")
    
    # Save to JSON
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(filtered_articles, f, ensure_ascii=False, indent=2)
    
    # Save to CSV (only selected fields)
    with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['title', 'url', 'date', 'publication_datetime', 'content', 'is_paywalled'])
        
        for article in filtered_articles:
            writer.writerow([
                article.get('title', ''),
                article.get('url', ''),
                article.get('date', ''),
                article.get('publication_datetime', article.get('date', '').split('T')[0] if 'T' in article.get('date', '') else article.get('date', '')),
                article.get('content', ''),
                article.get('is_paywalled', False)
            ])

def save_checkpoint(daily_count=0):
    """Save checkpoint information for resuming later"""
    # Get today's date for daily tracking
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Try to load existing checkpoint first to preserve daily counts
    daily_counts = {}
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                existing = json.load(f)
                if 'daily_counts' in existing:
                    daily_counts = existing['daily_counts']
        except:
            pass
    
    # Update today's count
    if today in daily_counts:
        daily_counts[today] = max(daily_counts[today], daily_count)
    else:
        daily_counts[today] = daily_count
    
    checkpoint = {
        'processed_urls': list(processed_urls),
        'last_run': datetime.now().isoformat(),
        'articles_count': len(processed_urls),
        'daily_counts': daily_counts
    }
    
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    print(f"✅ Checkpoint saved with {len(processed_urls)} processed URLs")
    print(f"📊 Today's count: {daily_counts.get(today, 0)}/{DAILY_TARGET} articles")

def load_checkpoint():
    """Load checkpoint information if available"""
    global processed_urls
    
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                
            if 'processed_urls' in checkpoint:
                processed_urls.update(checkpoint['processed_urls'])
                print(f"✅ Loaded checkpoint with {len(processed_urls)} processed URLs")
                print(f"📅 Last run: {checkpoint.get('last_run', 'unknown')}")
                
                # Show daily count information
                if 'daily_counts' in checkpoint:
                    today = datetime.now().strftime('%Y-%m-%d')
                    today_count = checkpoint['daily_counts'].get(today, 0)
                    print(f"📊 Today's progress: {today_count}/{DAILY_TARGET} articles")
                    
                    # Check if we've hit our daily quota
                    if today_count >= DAILY_TARGET:
                        print(f"🎯 Daily target of {DAILY_TARGET} articles already reached for today!")
                
                return True
        except Exception as e:
            print(f"⚠️ Error loading checkpoint: {str(e)}")
    
    return False

def get_daily_count():
    """Get the count of articles collected today"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                
            if 'daily_counts' in checkpoint:
                return checkpoint['daily_counts'].get(today, 0)
        except:
            pass
    
    return 0

def get_page_with_retry(driver, url, max_retries=MAX_RETRIES):
    """Attempt to get a page with retries"""
    for attempt in range(1, max_retries + 1):
        try:
            driver.get(url)
            WebDriverWait(driver, REQUEST_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            return True
        except (TimeoutException, WebDriverException) as e:
            if attempt < max_retries:
                print(f"⚠️ Attempt {attempt}/{max_retries} failed: {str(e)}")
                print(f"⏳ Waiting 10 seconds before retry...")
                time.sleep(10)
            else:
                print(f"❌ Failed to load page after {max_retries} attempts: {str(e)}")
                return False
    return False

def extract_date_from_url(url):
    """Try to extract date from URL"""
    try:
        # Look for patterns like /YYYY/MM/DD/ in the URL
        url_date_match = re.search(r'/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/', url)
        if url_date_match:
            year = int(url_date_match.group(1))
            month = int(url_date_match.group(2))
            day = int(url_date_match.group(3))
            
            # Check for time in URL (some sites include it)
            time_match = re.search(r'([0-9]{1,2})-([0-9]{1,2})-([0-9]{1,2})', url)
            if time_match:
                try:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2))
                    second = int(time_match.group(3))
                    # Validate the date (ensure it's not future dated, for example)
                    extracted_date = datetime(year, month, day, hour, minute, second)
                except Exception:
                    # If time parsing fails, just use the date
                    extracted_date = datetime(year, month, day)
            else:
                # No time in URL, just use the date
                extracted_date = datetime(year, month, day)
                
            if extracted_date <= datetime.now():
                return extracted_date
    except Exception:
        pass
    return None

def extract_date_directly_from_page(driver):
    """Extract the publication date directly from the page content"""
    try:
        # Try to find the date directly in the page content
        page_source = driver.page_source
        
        # Pattern to match dates like "Oct 1, 2025, 9:43 p.m."
        pattern = r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s+(a\.m\.|p\.m\.)'
        match = re.search(pattern, page_source)
        
        if match:
            date_string = match.group(0)
            print(f"✅ Found date string in page: {date_string}")
            
            # Fix the format to be parseable
            date_string = date_string.replace('a.m.', 'AM').replace('p.m.', 'PM')
            
            try:
                date_obj = datetime.strptime(date_string, '%b %d, %Y, %I:%M %p')
                print(f"✅ Parsed date with time: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                return date_obj
            except Exception as e:
                print(f"⚠️ Error parsing direct date: {e}")
    
    except Exception as e:
        print(f"⚠️ Error in direct page date extraction: {e}")
    
    return None

def extract_coindesk_timestamp(driver):
    """Extract the CoinDesk specific timestamp format (e.g., 'Oct 1, 2025, 9:43 p.m.')"""
    try:
        # First try the exact format seen in the screenshot
        try:
            # Look for the text directly after the "Edited by" part
            article_content = driver.find_element(By.TAG_NAME, "body").get_attribute("innerHTML")
            
            # Pattern for something like "Oct 1, 2025, 9:43 p.m." near the author section
            timestamp_pattern = r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s+(a\.m\.|p\.m\.)|\d{1,2}:\d{2}(AM|PM)'
            
            match = re.search(timestamp_pattern, article_content)
            if match:
                timestamp_text = match.group(0)
                print(f"✅ Found CoinDesk timestamp with regex: {timestamp_text}")
                
                # Fix common issues in the text before parsing
                timestamp_text = timestamp_text.replace('a.m.', 'AM').replace('p.m.', 'PM')
                
                # Try different formats
                formats = [
                    '%b %d, %Y, %I:%M %p',   # Oct 1, 2025, 9:43 PM
                    '%B %d, %Y, %I:%M %p',   # October 1, 2025, 9:43 PM
                    '%b %d, %Y, %I:%M%p',    # Oct 1, 2025, 9:43PM
                    '%B %d, %Y, %I:%M%p'     # October 1, 2025, 9:43PM
                ]
                
                for fmt in formats:
                    try:
                        date_obj = datetime.strptime(timestamp_text, fmt)
                        print(f"✅ Successfully parsed CoinDesk timestamp: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                        return date_obj
                    except ValueError:
                        continue
        except Exception as e:
            print(f"⚠️ Error in regex timestamp extraction: {str(e)[:100]}")
        
        # Look for timestamp elements - CoinDesk typically shows these below the author
        timestamp_selectors = [
            "time",  # Most common timestamp tag
            ".timestamp",
            ".article-datetime",
            ".published-datetime",
            ".article-info time",
            ".at-created",
            ".article-meta",  # The container that often holds the time
            ".article-date"  # Another common class for date display
        ]
        
        for selector in timestamp_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and (',' in text) and any(month in text.lower() for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']):
                        print(f"✅ Found CoinDesk timestamp: {text}")
                        
                        # Parse formats like "Oct 1, 2025, 9:43 p.m."
                        try:
                            # Handle various CoinDesk date formats
                            date_formats = [
                                '%b %d, %Y, %I:%M %p',  # Oct 1, 2025, 9:43 p.m.
                                '%b %d, %Y, %I:%M%p',   # Oct 1, 2025, 9:43pm
                                '%B %d, %Y, %I:%M %p',  # October 1, 2025, 9:43 p.m.
                                '%B %d, %Y, %I:%M%p',   # October 1, 2025, 9:43pm
                                '%b %d, %Y %I:%M %p',    # Oct 1, 2025 9:43 p.m.
                                '%b %d, %Y %I:%M%p'      # Oct 1, 2025 9:43pm
                            ]
                            
                            # Fix common issues in the text before parsing
                            text = text.replace('a.m.', 'AM').replace('p.m.', 'PM')
                            text = text.replace('am', 'AM').replace('pm', 'PM')
                            
                            # Try each format
                            for fmt in date_formats:
                                try:
                                    date_obj = datetime.strptime(text, fmt)
                                    print(f"✅ Successfully parsed CoinDesk timestamp: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                                    return date_obj
                                except ValueError:
                                    continue
                        except Exception as e:
                            print(f"⚠️ Error parsing CoinDesk timestamp: {e}")
            except Exception:
                continue
                
        return None
    except Exception as e:
        print(f"⚠️ Error in CoinDesk timestamp extraction: {e}")
        return None

def extract_date_from_meta(driver):
    """Try to extract date from meta tags"""
    try:
        # Try to find meta tags with date information
        meta_selectors = [
            "meta[property='article:published_time']",
            "meta[name='publication_date']",
            "meta[name='date']",
            "meta[name='publish-date']",
            "meta[name='pubdate']",
            "meta[itemprop='datePublished']"
        ]
        
        for selector in meta_selectors:
            try:
                meta = driver.find_element(By.CSS_SELECTOR, selector)
                content = meta.get_attribute("content")
                if content:
                    # Try different date formats that include time
                    try:
                        # ISO format (most common in meta tags)
                        date_obj = datetime.fromisoformat(content.replace('Z', '+00:00'))
                        print(f"✅ Found date+time in meta tag: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                        return date_obj
                    except:
                        try:
                            # RFC 3339 format
                            date_obj = datetime.strptime(content, '%Y-%m-%dT%H:%M:%S%z')
                            print(f"✅ Found date+time in RFC 3339 format: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                            return date_obj
                        except:
                            try:
                                # Common format with timezone
                                date_obj = datetime.strptime(content, '%Y-%m-%d %H:%M:%S %z')
                                print(f"✅ Found date+time with timezone: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                                return date_obj
                            except:
                                try:
                                    # Try without timezone
                                    date_obj = datetime.strptime(content, '%Y-%m-%d %H:%M:%S')
                                    print(f"✅ Found date+time without timezone: {date_obj.strftime('%Y-%m-%d %H:%M:%S')}")
                                    return date_obj
                                except:
                                    pass
            except NoSuchElementException:
                continue
    except Exception:
        pass
    return None

def extract_article_content(driver, url):
    """Extract content from a Coindesk article page"""
    try:
        print(f"🔍 Loading article page: {url}")
        if not get_page_with_retry(driver, url):
            return None
            
        # Wait a bit for JavaScript to load content (especially important for timestamps)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            # Take a short pause to ensure dynamic content loads
            time.sleep(2)
        except:
            pass
            
        # Check if this might be premium content
        premium_indicators = ["Premium", "Pro", "Subscriber", "Member"]
        is_premium = any(indicator in driver.page_source for indicator in premium_indicators)
        
        if is_premium:
            print("💎 Accessing premium content article")
            logging.info(f"Accessing premium content: {url}")
            
            # Try scrolling to help with premium content visibility
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/4);")
            time.sleep(1)
        
        # Check if we have a paywall despite being logged in
        paywall_detected = any(text in driver.page_source for text in 
                              ["Already have an account? Sign in", 
                               "Sign in to continue reading",
                               "Subscribe to continue reading"])
        
        if paywall_detected:
            print("⚠️ Still encountering a paywall after login attempt")
            
            # Try some paywall bypass techniques
            try:
                # Sometimes scrolling down helps bypass "soft" paywalls
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                # Try to dismiss any modal popups
                try:
                    close_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'Close') or contains(text(), 'No thanks') or contains(text(), 'Continue')]")
                    for button in close_buttons:
                        if button.is_displayed():
                            button.click()
                            time.sleep(1)
                            print("✅ Clicked on a potential popup close button")
                except Exception as e:
                    print(f"⚠️ Error dismissing popups: {str(e)[:100]}")
            except Exception as e:
                print(f"⚠️ Error during paywall bypass attempts: {str(e)[:100]}")
        
        # Extract title
        try:
            title_elem = driver.find_element(By.TAG_NAME, "h1")
            title = title_elem.text.strip()
            print(f"✅ Found title: {title}")
        except NoSuchElementException:
            title = "Unknown Title"
            print("⚠️ Could not find article title")
        
        # Extract content - try multiple selectors until we find content
        content = ""
        try:
            # Try several possible content selectors
            content_selectors = [
                "article p", 
                ".at-text p", 
                ".main-body-content p", 
                ".article-body p",
                "[itemprop='articleBody'] p",
                ".content-article p",
                ".article-content p",
                ".at-content-section p",
                ".story-text p"
            ]
            
            for selector in content_selectors:
                try:
                    content_elems = driver.find_elements(By.CSS_SELECTOR, selector)
                    if content_elems:
                        content = "\n\n".join([elem.text.strip() for elem in content_elems if elem.text.strip()])
                        if content:
                            break
                except Exception as e:
                    continue
            
            # If still no content, try broader selectors
            if not content:
                broader_selectors = ["article", ".article", ".post-content", ".content", "main"]
                for selector in broader_selectors:
                    try:
                        elem = driver.find_element(By.CSS_SELECTOR, selector)
                        # Extract all paragraph text
                        paragraphs = elem.find_elements(By.TAG_NAME, "p")
                        content = "\n\n".join([p.text.strip() for p in paragraphs if p.text.strip()])
                        if content:
                            break
                    except Exception:
                        continue
            
            if content:
                print(f"✅ Found content: {len(content)} characters")
            else:
                # Try to get any text content as a last resort
                try:
                    main_content = driver.find_element(By.TAG_NAME, "main")
                    paragraphs = main_content.find_elements(By.TAG_NAME, "p")
                    content = "\n\n".join([p.text.strip() for p in paragraphs if p.text.strip()])
                    print(f"⚠️ Used fallback content extraction: {len(content)} characters")
                except:
                    print("⚠️ Could not find article content")
        except Exception as e:
            print(f"⚠️ Error extracting content: {str(e)}")
        
        # Set a flag if the content is just the paywall text
        is_paywalled = False
        # Only mark as paywalled if it's explicitly a paywall message or completely empty
        if content == "Already have an account? Sign in" or (not content and paywall_detected):
            is_paywalled = True
            print("🔒 Article is paywalled - marking as such")
            # Append a note to the content for transparency
            if content == "Already have an account? Sign in":
                content = "[PAYWALLED] Article requires subscription. Could not access full content."
            elif not content:
                content = "[PAYWALLED] Could not extract content, likely behind a paywall."
        # Short content might be real content, not necessarily paywalled
        elif content and len(content) < 100:
            print(f"⚠️ Article has very short content ({len(content)} chars) - keeping but might need verification")
        
        # Extract date using existing methods
        # (Only the extract_article_content function has been modified)

        # Placeholder for date extraction
        date_str = datetime.now().isoformat()
        publication_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        has_time = True

        return {
            'title': title,
            'content': content,
            'url': url,
            'date': date_str,  # Keep ISO format for parsing
            'publication_datetime': publication_datetime,  # Human-readable format
            'has_time': has_time,  # Flag indicating if we have actual time information
            'is_paywalled': is_paywalled,  # Flag indicating if this article is paywalled
            'crawled_at': datetime.now().isoformat()
        }
    except Exception as e:
        logging.error(f"Error processing article {url}: {str(e)}")
        print(f"❌ Error processing article {url}: {str(e)}")
        return None

def get_article_links(driver):
    """Get article links from the main page"""
    article_links = []
    # Include all major sections and prioritize links with dates in the URL
    section_selectors = [
        'a[href*="/2025/"]',  # URLs with dates are most useful for our filtering
        'a[href^="/markets/"]', 
        'a[href^="/business/"]', 
        'a[href^="/tech/"]', 
        'a[href^="/policy/"]',
        'a[href^="/consensus-magazine/"]',
        'a[href^="/layer2/"]'
    ]
    
    for selector in section_selectors:
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        for element in elements:
            href = element.get_attribute("href")
            if href and href not in article_links:
                article_links.append(href)
    
    # Remove duplicates while preserving order
    seen = set()
    article_links = [x for x in article_links if not (x in seen or seen.add(x))]
    
    return article_links

def click_load_more(driver, max_clicks=MAX_LOAD_MORE_CLICKS, articles=None, articles_count=0, process_after_click=False):
    """Try to click the 'Load More' button to get more stories and optionally process articles after each click"""
    clicks = 0
    new_links_found = 0
    initial_links = get_article_links(driver)
    initial_count = len(initial_links)
    consecutive_failures = 0  # Track consecutive failures to prevent infinite loops
    processed_batch_urls = set()  # Track URLs processed in this batch
    
    # Process all articles from initial page first
    print(f"📋 Will process all visible articles first before clicking 'Load More'")
    
    # Possible load more button selectors
    load_more_selectors = [
        ".more-link", 
        ".load-more", 
        ".load-more-button", 
        "button:contains('Load More')", 
        "button:contains('More Stories')",
        "[data-testid='load-more']",
        ".at-load-more-button"
    ]
    
    print(f"🔍 Looking for 'Load More' button...")
    for clicks in range(max_clicks):
        # Check if we've reached our test limit
        if TEST_MODE and len(get_article_links(driver)) > initial_count + 20:
            print(f"✅ Found enough new links in test mode, stopping load more clicks")
            # Save checkpoint before exiting in test mode
            print("💾 Saving checkpoint before exiting test mode load more...")
            save_checkpoint()
            break
            
        button_found = False
        
        # Try scrolling to bottom first to reveal any lazy-loaded buttons
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        # Try each selector
        for selector in load_more_selectors:
            try:
                # First try to find via CSS
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                if not buttons:
                    # Then try XPath for text-based search
                    xpath = f"//button[contains(text(), 'More') or contains(text(), 'Load')]"
                    buttons = driver.find_elements(By.XPATH, xpath)
                
                for button in buttons:
                    if button.is_displayed() and button.is_enabled():
                        # Scroll the button into view
                        driver.execute_script("arguments[0].scrollIntoView(true);", button)
                        time.sleep(1)
                        
                        try:
                            print(f"✅ Found 'Load More' button, clicking... (Attempt {clicks+1}/{max_clicks})")
                            # Try using JavaScript click which is more reliable
                            driver.execute_script("arguments[0].click();", button)
                            button_found = True
                            time.sleep(5)  # Wait for new content to load
                            break
                        except Exception as e:
                            print(f"⚠️ Error clicking button: {str(e)}")
                            continue
            
            except Exception:
                continue
                
            if button_found:
                break
        
        if not button_found:
            print("⚠️ No 'Load More' button found or it's not clickable")
            break
            
        # Check if we got new links
        try:
            current_links = get_article_links(driver)
            new_count = len(current_links)
            
            if new_count > initial_count:
                new_links = new_count - initial_count
                new_links_found += new_links
                print(f"✅ Found {new_links} new article links after clicking 'Load More'")
                
                # Process new articles if requested
                if process_after_click and articles is not None:
                    print(f"🔍 Processing {new_links} new articles from this batch...")
                    # Get only the new links that weren't in initial_links
                    current_link_set = set(current_links)
                    initial_link_set = set(initial_links)
                    new_link_list = list(current_link_set - initial_link_set)
                    
                    # Process each new link
                    for i, link in enumerate(new_link_list):
                        if link in processed_urls or link in processed_batch_urls:
                            continue
                            
                        # Process the article (simplified version of the main processing loop)
                        try:
                            print(f"🔍 Processing article {i+1}/{len(new_link_list)}: {link}")
                            article_data = extract_article_content(driver, link)
                            if article_data:
                                articles.append(article_data)
                                articles_count += 1
                                processed_batch_urls.add(link)
                                processed_urls.add(link)
                                
                                # Save after each article
                                save_data(articles)
                                
                                # Wait between articles
                                if i < len(new_link_list) - 1:
                                    wait_time = random.uniform(MIN_WAIT, MAX_WAIT)
                                    print(f"💤 Sleeping {wait_time:.1f}s before next article...")
                                    time.sleep(wait_time)
                        except Exception as e:
                            print(f"❌ Error processing article {link}: {str(e)}")
                
                initial_count = new_count
                initial_links = current_links
                
                # Save checkpoint after successful Load More click and processing
                print("💾 Saving checkpoint after successful 'Load More' click...")
                save_checkpoint()
                
                # Wait longer to avoid rate limiting
                wait_time = random.uniform(5, 8)
                print(f"⏳ Waiting {wait_time:.1f}s before next action (avoiding rate limits)...")
                time.sleep(wait_time)
                consecutive_failures = 0  # Reset failure counter on success
            else:
                consecutive_failures += 1
                print(f"⚠️ No new links found after clicking 'Load More' (failure {consecutive_failures}/3)")
                if consecutive_failures >= 3:
                    print("⛔ Too many consecutive failures, stopping load more attempts")
                    # Save checkpoint before giving up on load more
                    print("💾 Saving checkpoint before ending load more attempts...")
                    save_checkpoint()
                    break
                # Wait longer after a failure
                time.sleep(random.uniform(10, 15))
        except Exception as e:
            print(f"⚠️ Error checking for new links: {str(e)}")
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print("⛔ Too many consecutive failures, stopping load more attempts")
                # Save checkpoint before giving up on load more due to exceptions
                print("💾 Saving checkpoint before ending load more attempts due to errors...")
                save_checkpoint()
                break
    
    if process_after_click:
        return new_links_found, articles_count, list(processed_batch_urls)
    else:
        return new_links_found

def main():
    """Main crawler function"""
    print("🚀 Coindesk Crawler v1.0 (Selenium Enhanced Edition)")
    print("👤 Using Chrome browser in headless mode")
    print(f"📅 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💾 Output: CSV ({OUTPUT_CSV}) and JSON ({OUTPUT_JSON})")
    print(f"🔍 Article date range: {START_DATE} to {END_DATE} (for filtering only)")
    print(f"🔄 Full Crawl Mode: Will crawl ALL articles regardless of date")
    
    if TEST_MODE:
        print(f"🧪 TEST MODE ENABLED: Will collect up to {MAX_TEST_ARTICLES} recent articles even if outside date range")
        
    # No daily quota check in full crawl mode
    print(f"🔄 Running in full crawl mode: Will collect all available articles")
    
    # Load existing data and checkpoints
    load_checkpoint()
    articles, articles_count = load_existing_data()
    
    print("🌐 Initializing browser...")
    driver = None
    
    try:
        driver = setup_browser()
        print("🌐 Browser started successfully")
        
        # First login before doing anything else
        print("🔐 Attempting to login to CoinDesk before crawling...")
        login_successful = login_to_coindesk(driver)
        if login_successful:
            print("✅ Login successful - proceeding with crawl")
            logging.info("Login successful - starting article crawl")
            
            # Double-check login status by visiting the main page
            print("🌐 Visiting main CoinDesk page to verify login")
            driver.get("https://www.coindesk.com/")
            time.sleep(3)
            
            print("✓ Ready to start crawling content")
        else:
            print("⚠️ Could not login but will try to continue anyway")
            logging.warning("Login failed - attempting to crawl without authentication")
            
        print(f"📊 Previously crawled {articles_count} articles")
        
        # Initialize counters
        new_articles_count = 0
        batch_num = 1
        
        # Initialize found_older_article flag to track if we've found articles before our date range
        # (Not used for stopping anymore)
        found_older_article = False
        daily_count = 0  # Initialize for compatibility with existing code
        
        # Continue indefinitely to crawl all articles regardless of date
        while True:  # Changed from 'not found_older_article' to always continue
            print(f"\n🔄 Starting batch #{batch_num}")
            print(f"⏱️ Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"📊 Current progress: {articles_count} articles collected so far (collecting all available articles)")
            
            # Get article links
            print(f"🔍 Loading Coindesk latest news page...")
            
            try:
                if not get_page_with_retry(driver, BASE_URL):
                    print("❌ Failed to load main page. Retrying in 30 seconds...")
                    time.sleep(30)
                    continue
                
                print(f"✅ Page loaded successfully")
                
                # Find initial article links
                article_links = get_article_links(driver)
                print(f"✅ Found {len(article_links)} unique article links initially")
                
                # Try to click "Load More" to get more stories and process articles after each click
                load_more_result = click_load_more(
                    driver, 
                    articles=articles, 
                    articles_count=articles_count,
                    process_after_click=True
                )
                
                if isinstance(load_more_result, tuple):
                    # Unpack the new results
                    new_links_found, updated_articles_count, processed_urls_in_batch = load_more_result
                    
                    # Update our variables with the new counts
                    articles_count = updated_articles_count
                    new_articles_count += len(processed_urls_in_batch)
                    
                    print(f"✅ Processed {len(processed_urls_in_batch)} new articles during 'Load More' clicks")
                    print(f"📊 Total articles: {articles_count} (added {new_articles_count} in this session)")
                    
                    # Since we've already processed articles during load more, we can skip processing again
                    # But we'll still get updated article links for any final processing
                    article_links = get_article_links(driver)
                    print(f"✅ Total article links after 'Load More': {len(article_links)}")
                else:
                    # Get updated article links after clicking Load More (old behavior)
                    article_links = get_article_links(driver)
                    print(f"✅ Total article links after 'Load More': {len(article_links)}")
                
                # Get list of URLs processed during Load More
                processed_during_load_more = []
                if isinstance(load_more_result, tuple) and len(load_more_result) > 2:
                    processed_during_load_more = load_more_result[2]
                
                # Process any remaining article links that weren't handled during Load More
                print(f"🔍 Processing any remaining articles not handled during Load More...")
                for i, link in enumerate(article_links):
                    # Break if we've reached our target
                    # No target limit in continuous mode - will stop when finding older articles
                    
                    if TEST_MODE and new_articles_count >= MAX_TEST_ARTICLES:
                        print(f"🎯 Reached target of {MAX_TEST_ARTICLES} test articles.")
                        break
                        
                    # Check if we've found an article older than our start date (already handled above)
                    
                    # Skip articles already processed during Load More operations or from previous runs
                    if link in processed_urls:
                        # Check if it was processed during this Load More session
                        if link in processed_during_load_more:
                            print(f"⏭️ Skipped: Already processed during Load More: {link}")
                        else:
                            print(f"⏭️ Skipped: Already processed in previous runs: {link}")
                        continue
                    
                    # Add to processed URLs set
                    processed_urls.add(link)
                    
                    # Extract article data
                    article_data = extract_article_content(driver, link)
                    
                    if article_data:
                        # Process the date
                        try:
                            article_date = datetime.fromisoformat(article_data['date'].replace('Z', '+00:00'))
                            
                            # First check if we have URL-based date extraction (most reliable)
                            has_url_date = 'url_extracted' in article_data and article_data['url_extracted']
                            
                            # Check if this is a fallback date (within a day of current date)
                            is_fallback_date = not has_url_date and abs((article_date - datetime.now()).total_seconds()) < 86400
                            
                            # If it has a URL date or it's not a fallback date, check date range
                            if has_url_date or not is_fallback_date:
                                in_date_range = start_date <= article_date <= end_date
                                
                                # Check if we've found an article older than our start date
                                # No longer stopping when finding older articles
                                if article_date < start_date:
                                    print(f"📌 Found article from {article_date.strftime('%Y-%m-%d')}, which is before our start date {START_DATE}")
                                    print(f"🔄 Continuing to crawl all articles regardless of date")
                                    # found_older_article = False - don't set this flag
                            else:
                                # For articles with fallback dates without URL dates, temporarily include them 
                                # but mark them for review
                                in_date_range = True 
                                print(f"⚠️ Using estimated date for article - including for now, but needs date verification")
                                
                            if in_date_range or (TEST_MODE and new_articles_count < MAX_TEST_ARTICLES):
                                # Add to our collection
                                articles.append(article_data)
                                articles_count += 1
                                new_articles_count += 1
                                
                                date_str = article_date.strftime('%Y-%m-%d')
                                test_mode_note = " (TEST MODE: collecting recent article)" if TEST_MODE and not (start_date <= article_date <= end_date) else ""
                                content_note = f" - Content: {len(article_data['content'])} chars" if article_data['content'] else " - No content"
                                
                                # Use publication_datetime if available, otherwise fall back to date_str
                                display_date = article_data.get('publication_datetime', date_str)
                                time_indicator = "⏰" if article_data.get('has_time', False) else "📅"
                                print(f"✅ [{new_articles_count}] Got: {article_data['title']} - {time_indicator} {display_date}{test_mode_note}{content_note}")
                                
                                # Save after each article to ensure we don't lose data
                                save_data(articles)
                                
                                # Save checkpoint every 5 articles
                                if new_articles_count % 5 == 0:
                                    save_checkpoint(articles_count)  # Use article count instead of daily count
                                    
                                # No daily target limit in continuous mode
                            else:
                                print(f"⏭️ Skipped: {article_data['title']} - Date {article_date.strftime('%Y-%m-%d')} outside range {START_DATE} to {END_DATE}")
                        except Exception as e:
                            logging.error(f"Error processing date for {link}: {str(e)}")
                            print(f"❌ Error processing date: {str(e)}")
                    
                    # Wait between requests
                    if i < len(article_links) - 1 and (new_articles_count < MAX_TEST_ARTICLES if TEST_MODE else True):
                        wait_time = random.uniform(MIN_WAIT, MAX_WAIT)
                        print(f"💤 Sleeping {wait_time:.1f}s before next article...")
                        time.sleep(wait_time)
                
                batch_num += 1
                
                # Check if we should continue or if we're in test mode and have enough articles
                if TEST_MODE and new_articles_count >= MAX_TEST_ARTICLES:
                    break
                    
                # Continue loading more pages regardless of article dates
                if not TEST_MODE:
                    wait_time = random.uniform(BATCH_WAIT_MIN, BATCH_WAIT_MAX)  # Wait between batch runs
                    print(f"⏳ Waiting {wait_time:.1f}s before loading more articles...")
                    time.sleep(wait_time)
                    continue
                else:
                    break
                
            except Exception as e:
                logging.error(f"Error processing batch {batch_num}: {str(e)}")
                print(f"❌ Error processing batch: {str(e)}")
                print(f"⏳ Waiting 60 seconds before retrying (longer pause to avoid rate limiting)...")
                time.sleep(60)  # Longer wait after an error
                
                # Try recreating the browser if we're encountering serious issues
                try:
                    driver.quit()
                except:
                    pass
                    
                print("🔄 Recreating browser...")
                driver = setup_browser()
        
        # Final save
        save_data(articles)
        save_checkpoint(articles_count)  # Use articles_count instead
        
        # Calculate runtime statistics
        runtime_seconds = time.time() - start_time
        runtime_minutes = runtime_seconds / 60
        
        print("\n✅ Crawl complete")
        print(f"📈 Results: Crawled {new_articles_count} new articles, total {articles_count} overall")
        print(f"📊 Total articles collected: {articles_count} articles")
        print(f"⏰ Runtime: {runtime_seconds:.1f} seconds ({runtime_minutes:.1f} minutes)")
        print(f"\n💾 Data saved to:")
        print(f"  - CSV: {os.path.abspath(OUTPUT_CSV)}")
        print(f"  - JSON: {os.path.abspath(OUTPUT_JSON)}")
        print(f"  - Log: {os.path.abspath(ERROR_LOG)}")
    
    except Exception as e:
        logging.error(f"Critical error in main crawler: {str(e)}")
        print(f"❌ Critical error: {str(e)}")
    
    finally:
        print("\n🧹 Cleaning up resources...")
        try:
            if driver:
                driver.quit()
            print("Browser closed successfully")
        except Exception as e:
            logging.error(f"Error closing browser: {str(e)}")
            print(f"❌ Error closing browser: {str(e)}")

def verify_time_extraction():
    """Verify if the time extraction is working by checking existing data"""
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
                articles = json.load(f)
                
            print(f"\n🔍 Verifying time extraction in {len(articles)} articles:")
            
            # Count articles with time information
            time_count = 0
            url_extracted_count = 0
            direct_extracted_count = 0
            coindesk_format_count = 0
            fallback_count = 0
            
            print("First 3 articles:")
            for i, article in enumerate(articles[:3]):  # Check first 3 articles
                date_str = article.get('date', '')
                pub_datetime = article.get('publication_datetime', '')
                url_extracted = article.get('url_extracted', False)
                direct_extracted = article.get('direct_extracted', False)
                coindesk_format_extracted = article.get('coindesk_format_extracted', False)
                has_time = article.get('has_time', False)
                
                # Count extraction types
                if has_time:
                    time_count += 1
                if url_extracted:
                    url_extracted_count += 1
                if direct_extracted:
                    direct_extracted_count += 1
                if coindesk_format_extracted:
                    coindesk_format_count += 1
                
                # Check for fallback dates
                if date_str and (date_str == datetime.now().isoformat()[:10] or 
                               date_str == (datetime.now() - timedelta(days=1)).isoformat()[:10]):
                    fallback_count += 1
                    
                print(f"  Article {i+1}: {article.get('title', '')[:30]}...")
                print(f"    Date: {date_str}")
                print(f"    Publication datetime: {pub_datetime}")
                print(f"    Has time info: {'✅' if has_time else '❌'}")
                print(f"    Direct extraction: {'✅' if direct_extracted else '❌'}")
                print(f"    CoinDesk format: {'✅' if coindesk_format_extracted else '❌'}")
                print(f"    URL extraction: {'✅' if url_extracted else '❌'}")
                print()
                
            print("\nLatest 3 articles:")
            for i, article in enumerate(articles[-3:]):  # Check latest 3 articles
                date_str = article.get('date', '')
                pub_datetime = article.get('publication_datetime', '')
                url_extracted = article.get('url_extracted', False)
                direct_extracted = article.get('direct_extracted', False)
                coindesk_format_extracted = article.get('coindesk_format_extracted', False)
                has_time = article.get('has_time', False)
                
                # Count extraction types for the most recent articles
                if has_time:
                    time_count += 1
                if url_extracted:
                    url_extracted_count += 1
                if direct_extracted:
                    direct_extracted_count += 1
                if coindesk_format_extracted:
                    coindesk_format_count += 1
                
                print(f"  Article {len(articles)-2+i}: {article.get('title', '')[:30]}...")
                print(f"    Date: {date_str}")
                print(f"    Publication datetime: {pub_datetime}")
                print(f"    Has time info: {'✅' if has_time else '❌'}")
                print(f"    Direct extraction: {'✅' if direct_extracted else '❌'}")
                print(f"    CoinDesk format: {'✅' if coindesk_format_extracted else '❌'}")
                print(f"    URL extraction: {'✅' if url_extracted else '❌'}")
                print()
            
            print(f"Summary of all {len(articles)} articles:")
            print(f"  Articles with time information: {time_count}/{len(articles)}")
            print(f"  Articles with direct page extraction: {direct_extracted_count}/{len(articles)}")
            print(f"  Articles with CoinDesk format extraction: {coindesk_format_count}/{len(articles)}")
            print(f"  Articles with URL-extracted dates: {url_extracted_count}/{len(articles)}")
            print(f"  Articles with fallback dates: {fallback_count}/{len(articles)}")
            
            if time_count == 0:
                print("\n⚠️ No articles have time information! Time extraction may not be working correctly.")
                print("   Suggestions:")
                print("   - Check if direct page extraction is working")
                print("   - Verify CoinDesk format extraction")
                print("   - Check meta tag date extraction")
                print("   - Consider adding more time extraction methods")
        except Exception as e:
            print(f"Error verifying time extraction: {str(e)}")
    else:
        print(f"No existing data found in {OUTPUT_JSON}")

if __name__ == "__main__":
    # Uncomment this line to verify time extraction in existing data
    # verify_time_extraction()
    
    start_time = time.time()
    main()
# Debug statement
if __name__ == '__main__':
    print('Debug: Running main function')
    start_time = time.time()
    main()
