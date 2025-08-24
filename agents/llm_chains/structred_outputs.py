from typing_extensions import Literal,List
from pydantic import BaseModel, Field


class decision(BaseModel):
    search_type:Literal['by_category','by_search_term']=Field(description="whether the search type should be by category or search term (keyword)")
    query_term:str=Field(description="the specic search term for category or the specific keyword for keyword search, #Important: the term should be only one word")

class news_summarizer(BaseModel):
    heading:str=Field(content="Relevant Heading")
    summary: str=Field(content='Human readable summary')