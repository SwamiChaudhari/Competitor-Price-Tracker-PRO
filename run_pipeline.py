import subprocess
import logging
from datetime import datetime
import os

# --- Configuration ---
# Use absolute path for your project folder
BASE_DIR = r'D:\Competitor-Price-Tracker-PRO'
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Create logs directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)

# Setup logging
log_filename = os.path.join(LOG_DIR, f'pipeline_{datetime.now().strftime("%Y-%m-%d")}.log')

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

def run_script(script_name):
    try:
        logging.info(f"Starting {script_name}...")
        # Use full path to the script to ensure it runs correctly
        script_path = os.path.join(BASE_DIR, script_name)
        
        # Runs the script and waits for it to finish
        result = subprocess.run(['python', script_path], check=True, capture_output=True, text=True)
        logging.info(f"Successfully finished {script_name}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {script_name}: {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error running {script_name}: {str(e)}")
        return False

if __name__ == "__main__":
    logging.info("--- Pipeline Started ---")
    
    # 1. Scraper
    if run_script('scraper.py'):
        # 2. ETL
        if run_script('etl_pipeline.py'):
            # 3. Alerts
            run_script('alert_system.py')
            
    logging.info("--- Pipeline Finished ---")