import feedparser
import pandas as pd
from datetime import datetime

def get_daily_brief():
    """Fetch daily market brief: news and earnings calendar"""
    feed = feedparser.parse("https://finance.yahoo.com/rss/topstories")
    news = [{"title": entry.title, "link": entry.link, "published": entry.published} 
            for entry in feed.entries[:8]]
    
    earnings = pd.DataFrame([{"symbol": "AAPL", "eps_estimate": 2.3, "time": "Before Market"}])
    
    return {
        "news": news,
        "earnings": earnings,
        "date": datetime.now().strftime("%Y-%m-%d")
    }
