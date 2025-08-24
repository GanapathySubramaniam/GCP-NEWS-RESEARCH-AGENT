from langgraph.graph import StateGraph
from .states import State
from .nodes import decision_agent,news_summarizer,assign_workers,synthesizer
from langgraph.graph import START,END

workflow = StateGraph(State)

workflow.add_node("orchestrator", decision_agent)
workflow.add_node("worker", news_summarizer)
workflow.add_node("synthesizer", synthesizer)

workflow.add_edge(START, "orchestrator")
workflow.add_conditional_edges("orchestrator", assign_workers,['worker']) 
workflow.add_edge("worker", "synthesizer")
workflow.add_edge("synthesizer", END)

graph= workflow.compile()