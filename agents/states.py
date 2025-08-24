from operator import add
from pydantic import BaseModel, Field
from typing import List,Annotated,TypedDict
from langchain_core.messages import AnyMessage
from langgraph.graph import add_messages

class WorkerState(TypedDict):
    worker_section: List[AnyMessage]
    completed_sections: Annotated[list, add]

class State(TypedDict):
    messages:Annotated[list[AnyMessage], add_messages]
    planned_sections: List[AnyMessage]
    completed_sections:Annotated[List,add]
 