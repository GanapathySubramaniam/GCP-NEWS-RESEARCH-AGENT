import requests
import pandas as pd
from datetime import datetime
import time
import os 


category_queries = {
    'AI': 'artificial intelligence OR machine learning OR AI',
    'business': 'business OR finance OR economy OR market',
    'politics': 'politics OR election OR government OR policy',
    'sports': 'sports OR football OR basketball OR soccer',
    'tech': 'technology OR software OR startup OR innovation',
    'science': 'science OR research OR discovery OR study',
    'entertainment': 'entertainment OR movies OR music OR celebrity'
}

def fetch_news_search(category, query, num_results=20):
    """
    Fetch news using Google search news tab
    """
    params = {
        "api_key": os.environ['SERPAPI_api_key'],
        "engine": "google",
        "tbm": "nws",  # News tab
        "q": query,
        "gl": "us",
        "hl": "en",
        "num": 100,  # Get more results to filter
        "tbs": "qdr:w"  # Past week for fresh news
    }
    
    try:
        response = requests.get("https://serpapi.com/search", params=params)
        response.raise_for_status()
        data = response.json()
        
        # Get news results and limit to desired number
        news_results = data.get("news_results", [])[:num_results]
        return news_results
        
    except Exception as e:
        print(f"Error fetching news for {category}: {e}")
        return []

def collect_news():
    all_news = []
    print("🔍 Starting alternative news collection...")
    
    for category, query in category_queries.items():
        print(f"📰 Searching for {category} news...")
        
        news_results = fetch_news_search(category, query, num_results=20)
        
        for item in news_results:
            all_news.append({
                "category": category,
                "headline": item.get("title", "No title available"),
                "url": item.get("link", "No URL available")})
        
        print(f"✅ Found {len(news_results)} articles for {category}")
        time.sleep(0.5)
    news_df=pd.DataFrame(all_news)
    data_path='./data/raw_news.csv'
    news_df.to_csv(data_path,index=False)
    return f"\n📊 results: {news_df.shape[0]} articles extracted and stored in {data_path}"


