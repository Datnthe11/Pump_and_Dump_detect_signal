Crypto Pump-and-Dump Signal Detection Toolkit

This repository provides tools to detect pump-and-dump signals in cryptocurrency markets by combining news articles, social media activity, and trading data.

🔹 Main Goal

• Detect short-term pump and dump events in crypto markets.
• Provide high-resolution labeling and features for machine learning models.
• Integrate multiple data sources: news (CoinDesk), tweets, and OHLCV trading data.

🔹 Key Components 

- CoinDesk Article Crawler:
• Fetches crypto news with title, content, and exact publication time
• Optional login for paywalled content

- Twitter/X Scraper:
• Collect tweets related to crypto hype
• Filter by keywords, engagement, and language
• Generate social media features for pump-and-dump analysis

- OHLCV Pump/Dump Labeling:
• Label minute-level trading data as pump, dump, or neutral
• Configurable price and volume thresholds
• Provides helper metrics to explain labels

- Data Integration:
• Combine news, tweets, and labeled Bitcoin trading data
• Create features for ML models (e.g., LightGBM) using embeddings from a BiLSTM + Attention feature extractor
• Enable hybrid analysis of social media and market signals

🔹 Who Should Use This Repo

• Crypto analysts and researchers studying pump-and-dump schemes
• Developers building predictive models for short-term market movements
• Anyone interested in linking social sentiment and trading behavior
