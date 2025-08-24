from google.cloud import language_v1
import requests
import pandas as pd
from newspaper import Article
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

class GCPContentPreprocessor:
    def __init__(self):
        self.nlp_client = language_v1.LanguageServiceClient()
        self.article_count = 1

    def process_webpage(self, url):
        # Always return 5 items! (url, extracted_text, sentiment, entities, category)
        error_return = pd.Series(
            [url, "ERROR", "ERROR", "ERROR", "ERROR"],
            index=['url', 'extracted_text', 'sentiment', 'entities', 'detailed_category']
        )
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            }
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                err = f"Failed to fetch page: {response.status_code}"
                print(f"{url}: {err}")
                return pd.Series(
                    [url, err, err, err, err],
                    index=['url', 'extracted_text', 'sentiment', 'entities', 'detailed_category']
                )
            article = Article(url)
            article.download(input_html=response.text)
            article.parse()
            clean_text = article.text.strip()
            # Heuristic: paywalled/restricted content or too short
            short_flag = (
                not clean_text
                or len(clean_text.split()) < 30
                or any(kw in clean_text.lower() for kw in [
                    "subscribe", "sign in", "registration required",
                    "become a member", "this content is for subscribers"
                ])
            )
            if short_flag:
                err = "Content too short or access restricted"
                print(f"{url}: {err}")
                return pd.Series(
                    [url, err, err, err, err],
                    index=['url', 'extracted_text', 'sentiment', 'entities', 'detailed_category']
                )
            try:
                # Analyze content using GCP NLP API
                text_insights = self.analyze_text_content(clean_text)
                text_insights['extracted_text'] = clean_text
            except Exception as e:
                print(f"{url}: GCP NLP error\n{traceback.format_exc()}")
                err = f"GCP NLP ERROR: {e}"
                return pd.Series(
                    [url, clean_text, err, err, err],
                    index=['url', 'extracted_text', 'sentiment', 'entities', 'detailed_category']
                )
            print(f"Article {self.article_count} extraction complete")
            self.article_count += 1
            return pd.Series(
                [url, text_insights['extracted_text'], text_insights['sentiment'],
                 text_insights['entities'], text_insights['detailed_category']],
                index=['url', 'extracted_text', 'sentiment', 'entities', 'detailed_category']
            )
        except Exception as e:
            print(f"{url}: {e}\n{traceback.format_exc()}")
            return error_return

    def analyze_text_content(self, text):
        document = language_v1.Document(
            content=text, type_=language_v1.Document.Type.PLAIN_TEXT
        )
        sentiment_info = self.nlp_client.analyze_sentiment(
            request={'document': document}
        ).document_sentiment
        sentiment = f"Sentiment score {sentiment_info.score} , Sentiment Magnitude {sentiment_info.magnitude}"
        entities_resp = self.nlp_client.analyze_entities(request={'document': document})
        entities = entities_resp.entities if hasattr(entities_resp, 'entities') else []
        if entities:
            entities = sorted(entities, key=lambda x: x.salience, reverse=True)
            MAIN_ENTITY_TYPES = {1, 2, 3, 4, 5, 6}
            entities_info = {}
            for entity in entities:
                if entity.type_ in MAIN_ENTITY_TYPES and entity.name.lower() not in entities_info:
                    entities_info[entity.name] = entity
            top_main_entities = list(entities_info.values())[:20]
            entities_str = ', '.join(
                [f"name:{entity.name} type: {entity.type_} Salience {entity.salience}" for entity in top_main_entities]
            )
        else:
            entities_str = 'None'
        classification = self.nlp_client.classify_text(request={'document': document})
        if hasattr(classification, 'categories') and classification.categories:
            category_info = sorted(classification.categories, key=lambda x: x.confidence, reverse=True)[0]
            category = f"Category:{category_info.name} , Category_confidence:{round(category_info.confidence*100,2)}%"
        else:
            category = 'Category:None , Category_confidence:0%'
        return {
            'sentiment': sentiment,
            'entities': entities_str,
            'detailed_category': category
        }

def parallel_apply(urls, func, max_workers=15):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(func, url): url for url in urls}
        for future in as_completed(future_to_url):
            try:
                results.append(future.result())
            except Exception as e:
                # Always return full error shape
                results.append(
                    pd.Series(
                        [future_to_url[future], "ERROR", "ERROR", "ERROR", "ERROR"],
                        index=['url', 'extracted_text', 'sentiment', 'entities', 'detailed_category']
                    )
                )
    return results

def extract_and_preprocess():
    preprocess_object = GCPContentPreprocessor()
    news_df = pd.read_csv('./data/raw_news.csv')
    results = parallel_apply(news_df['url'], preprocess_object.process_webpage, max_workers=15)
    results_df = pd.DataFrame(results, columns=['url','extracted_text','sentiment','entities','detailed_category'])
    results_df.to_csv('./data/preprocessed_news.csv', index=False)
    return 'Preprocessed and data stored in ./data/preprocessed_news.csv'
