@echo off
cd /d D:\Competitor-Price-Tracker-PRO
python scraper.py
python etl_pipeline.py
pause