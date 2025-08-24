import pandas as pd


def combine_data():
    news_df=pd.read_csv('./data/raw_news.csv')
    preprocessed_news_df=pd.read_csv('./data/preprocessed_news.csv')
    ERROR_STRINGS = ["Failed to fetch page","Content too short or access restricted","ERROR","GCP NLP ERROR"]
    mask = preprocessed_news_df[['extracted_text', 'sentiment', 'entities', 'detailed_category']].apply(lambda s: ~s.astype(str).str.contains('|'.join(ERROR_STRINGS)), axis=1).all(axis=1)
    pd.merge(news_df,preprocessed_news_df[mask],on='url',how='inner').to_csv('./data/news.csv',index=False)
    return 'Data combined at ./data/news.csv '