from __future__ import annotations

BASE_URL = "https://www.flipkart.com"
LISTING_URL = "https://www.flipkart.com/mobiles/pr?sid=tyy,4io"

# Amazon
AMAZON_BASE_URL = "https://www.amazon.in"
AMAZON_LISTING_URL = "https://www.amazon.in/s?k=mobiles"

# Scrape targets
TARGET_COUNT = 50
AMAZON_TARGET_COUNT = 50
TOTAL_TARGET_COUNT = 100
MAX_PAGES = 10  # safety cap (listing pages)

# Networking
TIMEOUT_SECONDS = 25
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 1.2  # exponential backoff base

# Politeness
DELAY_RANGE_SECONDS = (2.0, 5.0)

# Use exactly 3 user agents (requirement)
USER_AGENTS = [
    # Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    # Safari (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

# Output locations
RAW_DATA_DIR = "raw_data"
LOGS_DIR = "logs"

# Optional: restrict to only product URLs containing this segment
MOBILES_PATH_PREFIX = "/"


# Alerts (Gmail SMTP)
# NOTE: Keep app passwords out of git history.
GMAIL_SMTP_USER = "Swami1642004@gmail.com"
GMAIL_APP_PASSWORD = "smgr ukum fxpj rgwu"
ALERT_EMAIL_TO = "Swami1642004@gmail.com"
ALERT_EMAIL_FROM = "Swami1642004@gmail.com"

