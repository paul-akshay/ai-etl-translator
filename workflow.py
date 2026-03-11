from typing import TypedDict
from langgraph.graph import StateGraph

from parser import xml_to_metadata
from spark_code_generator import generate_spark_code


from typing import TypedDict

class GraphState(TypedDict):
    xml: str
    metadata: dict
    approved: bool
    spark_code: str


def parser_agent(state):

    metadata = xml_to_metadata(state["xml"])

    return {"metadata": metadata}

def approval_node(state):

    print("\nGenerated Canonical Metadata:\n")
    print(state["metadata"])

    approval = input("\nApprove metadata? (y/n): ")

    if approval.lower() == "y":
        return {"approved": True}
    else:
        raise Exception("Metadata rejected. Stopping workflow.")


def spark_generator_node(state):

    spark_code = generate_spark_code(state["metadata"])

    return {"spark_code": spark_code}


workflow = StateGraph(GraphState)

workflow.add_node("parser", parser_agent)
workflow.add_node("approval", approval_node)
workflow.add_node("spark_generator", spark_generator_node)

workflow.set_entry_point("parser")

workflow.add_edge("parser", "approval")
workflow.add_edge("approval", "spark_generator")

app = workflow.compile()