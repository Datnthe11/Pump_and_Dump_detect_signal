# Crypto Pump-and-Dump Signal Detection Toolkit

## 🚀 Overview
A machine learning system for detecting short-term pump-and-dump events in cryptocurrency markets by integrating news articles, social media signals, and trading data.

The project focuses on multi-modal feature engineering and high-resolution event labeling to improve detection of abnormal market movements.

---

## 🎯 Objectives
- Detect short-term pump-and-dump patterns in crypto markets  
- Build high-resolution labeled datasets for machine learning models  
- Combine textual sentiment (news, tweets) with market signals (OHLCV)  

---

## 🧱 System Architecture

### 1. Data Collection
- Crawl crypto news from CoinDesk (timestamp-aligned)  
- Scrape Twitter/X data with keyword and engagement filtering  
- Collect OHLCV trading data at minute-level resolution  

### 2. Labeling Pipeline
- Generate pump / dump / neutral labels based on price and volume thresholds  
- Provide interpretable metrics to support labeling decisions  

### 3. Feature Engineering
- Extract text embeddings using BiLSTM + Attention  
- Combine with market indicators (price, volume, volatility)  
- Construct multi-modal feature sets  

### 4. Modeling
- Train machine learning models (e.g., LightGBM) for event detection  
- Evaluate model performance on labeled data  

---

## 🛠️ Tech Stack
- **Language:** Python  
- **ML/DL:** PyTorch (BiLSTM + Attention), LightGBM  
- **Data:** NLP, Time-series analysis  
- **Pipeline:** Data crawling, feature engineering, labeling system  

---

## 📊 Key Highlights
- Built an end-to-end ML pipeline from raw data collection to model training  
- Designed a high-resolution labeling framework for financial time-series data  
- Developed multi-modal features combining social sentiment and market signals  

---

## 📌 Repository Purpose
This repository is developed for research and portfolio purposes, demonstrating practical applications of machine learning in financial market analysis.

---
