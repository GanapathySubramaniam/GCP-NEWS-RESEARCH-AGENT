from langchain_core.prompts import ChatPromptTemplate

with open('./agents/llm_chains/prompts/decision_llm.txt') as f:
    decision_prompt=f.read()

with open('./agents/llm_chains/prompts/news_summarizer.txt')as f:
    news_summarizer=f.read()
    
decision_prompt_template=ChatPromptTemplate.from_messages([("system",decision_prompt),("placeholder","{messages}")])

news_summarizer_prompt_template=ChatPromptTemplate.from_messages([("system",news_summarizer),("placeholder","{messages}")])

