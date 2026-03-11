from typing import TypedDict, Optional
from langgraph.graph import StateGraph

from parsers.powercenter_parser import pc_to_metadata
from parsers.abinitio_parser import abinitio_to_metadata
from codegenerators.spark_code_generator import generate_spark_code

# import constants from main
from config import PARSER_CHOSEN
from constants import (AB_INITIO, POWERCENTER)

# ---------------------------
# STATE DEFINITIONS
# ---------------------------

class MetadataState(TypedDict, total=False):
    xml: Optional[str]
    graph: Optional[str]
    schema: Optional[str]
    transform: Optional[str]
    metadata: dict


class ETLState(TypedDict):
    metadata: dict
    spark_code: str


# ---------------------------
# PARSER NODE
# ---------------------------

def parser_node(state: MetadataState):

    if PARSER_CHOSEN == POWERCENTER:

        metadata = pc_to_metadata(state["xml"])

    elif PARSER_CHOSEN == AB_INITIO:

        metadata = abinitio_to_metadata(
            state["graph"],
            state["schema"],
            state["transform"]
        )

    else:
        raise Exception("Invalid parser selected")

    return {
        "metadata": metadata
    }


# ---------------------------
# METADATA WORKFLOW
# ---------------------------

metadata_workflow = StateGraph(MetadataState)

metadata_workflow.add_node("parser", parser_node)

metadata_workflow.set_entry_point("parser")

metadata_app = metadata_workflow.compile()


# ---------------------------
# ETL WORKFLOW
# ---------------------------

def spark_generator_node(state: ETLState):

    spark_code = generate_spark_code(state["metadata"])

    return {
        "spark_code": spark_code
    }


etl_workflow = StateGraph(ETLState)

etl_workflow.add_node("spark_generator", spark_generator_node)

etl_workflow.set_entry_point("spark_generator")

etl_app = etl_workflow.compile()