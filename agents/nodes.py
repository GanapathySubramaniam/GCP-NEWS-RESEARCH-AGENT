from .states import State,WorkerState
from .llm_chains import decision_chain,news_summarizer_chain
from .tools import toolkit
from langgraph.constants import Send
from langchain_core.messages import AIMessage,HumanMessage

def ensure_last_message_is_user(messages):
    if not isinstance(messages, list):
        messages = [messages]
    while messages and not isinstance(messages[-1], HumanMessage):
        messages.pop()
    if not messages or not isinstance(messages[-1], HumanMessage):
        raise ValueError("Gemini API requires the last message to be from the user.")
    return messages


def decision_agent(state: State):
    decision=decision_chain.invoke({"messages":state["messages"]})
    if decision.search_type=="by_category":
        search_results=[HumanMessage(content=f'{i["headline"]} \n{i["extracted_text"]}\nSentiment:{i["sentiment"]}\nEntities:{i["entities"]}' )for i in toolkit.get_news_by_category(decision.query_term)]
  
    elif decision.search_type=="by_search_term":
        search_results=[HumanMessage(content=f'{i["headline"]} \n{i["extracted_text"]}\nSentiment:{i["sentiment"]}\nEntities:{i["entities"]}' ) for i in toolkit.get_news_by_search_term(decision.query_term)]
    return {"planned_sections": search_results}

def news_summarizer(workerstate: WorkerState):
    section = workerstate['worker_section']
    if not isinstance(section, list):
        section = [section]
    output = news_summarizer_chain.invoke({"messages": section})
    output = AIMessage(content=f'# {output.heading} \n {output.summary}')
    return {"completed_sections": [output]}


def assign_workers(state: State):
    print('assinging workers')
    return [Send("worker", {"worker_section": section}) for section in state["planned_sections"]]

def synthesizer(state: State):
    print('synthesizing')
    completed_sections=[i.content for i in state['completed_sections']]
    completed_report_sections = "\n\n---\n\n".join(completed_sections)
    return {"messages": completed_report_sections}