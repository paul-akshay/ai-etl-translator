from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
import json

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)


def generate_spark_code(metadata):

    prompt = f"""
Generate executable Spark Scala ETL code from the following canonical ETL metadata. without markdown

Requirements:
- Use Spark DataFrame API

Metadata:
{json.dumps(metadata, indent=2)}

Return ONLY Spark Scala code.
"""

    response = llm.invoke([
        HumanMessage(content=prompt)
    ])

    return response.content