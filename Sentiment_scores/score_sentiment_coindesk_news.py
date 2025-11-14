import json
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from tqdm import tqdm
from pathlib import Path

def analyze_sentiment_with_finbert(input_file, output_file):
    """
    Analyze sentiment in articles using FinBERT and save results to a new file.
    
    Args:
        input_file (str): Path to input JSON file with labeled articles
        output_file (str): Path to output JSON file with sentiment analysis added
    """
    print(f"Loading data from {input_file}...")
    
    # Load the labeled JSON data
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    total_articles = len(data)
    print(f"Total articles to analyze: {total_articles}")
    
    # Load FinBERT model and tokenizer
    print("Loading FinBERT model...")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")
    
    model_name = "ProsusAI/finbert"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    model.to(device)
    print("FinBERT model loaded successfully")
    
    # Process each article
    sentiment_counts = {
        "positive": 0,
        "negative": 0,
        "neutral": 0
    }
    
    # Add save interval to periodically save results when processing large datasets
    save_interval = 500  # Save results every 500 articles
    last_save = 0
    
    for i, article in tqdm(enumerate(data), total=total_articles, desc="Processing articles"):
        # Skip articles without content
        if 'content' not in article or not article['content'].strip():
            article['sentiment_score'] = 0.0  # Neutral sentiment score
            sentiment_counts["neutral"] += 1
            continue
        
        # Get content and hype/fundamental label
        content = article['content']
        is_hype = article.get('is_hype', 0) == 1
        
        # Create enriched text with context as suggested
        # This helps FinBERT understand the financial context better
        enriched_text = f"{content} This article is about {'market hype' if is_hype else 'fundamental analysis'}."
        
        # Truncate if too long
        if len(enriched_text) > 2000:
            enriched_text = enriched_text[:2000]
        
        # Tokenize and prepare for model
        inputs = tokenizer(enriched_text, return_tensors="pt", truncation=True, 
                          max_length=512, padding=True).to(device)
        
        # Get sentiment prediction
        with torch.no_grad():
            outputs = model(**inputs)
            scores = torch.nn.functional.softmax(outputs.logits, dim=1)
            scores = scores[0].cpu().numpy().tolist()  # Convert to regular list
        
        # Calculate a more nuanced sentiment score regardless of the dominant class
        # This creates a score from -1 to +1 based on the relative strength of positive vs negative
        sentiment_score = round(scores[0] - scores[1], 3)
        
        # Still determine the dominant class for counting/classification purposes
        max_index = scores.index(max(scores))
        if max_index == 0:
            sentiment_label = "positive"
        elif max_index == 1:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"
        
        # Add just the sentiment score to article
        article['sentiment_score'] = sentiment_score
        
        # Update sentiment counts (for reporting only)
        sentiment_counts[sentiment_label] += 1
        
        # Periodically save results
        if (i + 1) % save_interval == 0 and i + 1 > last_save:
            print(f"\nIntermediate save at {i + 1}/{total_articles} articles")
            
            # Calculate current average sentiment score
            current_avg_score = sum(data[j].get('sentiment_score', 0) for j in range(i+1)) / (i+1)
            
            print(f"Current average sentiment score: {current_avg_score:.3f} (scale -1 to +1)")
            
            # Save intermediate results
            temp_output_file = str(output_file).replace(".json", f"_partial_{i+1}.json")
            with open(temp_output_file, 'w', encoding='utf-8') as f:
                json.dump(data[:i+1], f, indent=2, ensure_ascii=False)
            print(f"Intermediate results saved to {temp_output_file}")
            last_save = i + 1
    
    # Print summary statistics
    print(f"\nSentiment analysis complete:")
    
    # Calculate average normalized score
    total_score = sum(article.get('sentiment_score', 0) for article in data)
    avg_score = total_score / total_articles if total_articles > 0 else 0
    print(f"Average sentiment score: {avg_score:.3f} (on a scale from -1 to +1)")
    
    # Save the labeled data
    print(f"Saving results to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Sentiment analysis complete. Results saved to {output_file}")
    
    # Return sentiment distribution for quick reference
    return sentiment_counts

if __name__ == "__main__":
    # File paths
    input_file = Path("coindesk_jul_sep_2025_labeled.json")
    output_file = Path("coindesk_jul_sep_2025_sentiment.json")
    
    # Run sentiment analysis
    analyze_sentiment_with_finbert(input_file, output_file)
    print("Done!")