import os
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from dotenv import load_dotenv

load_dotenv()

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0
)


def xml_to_metadata(xml_text: str):

    prompt = f"""
You are an ETL migration expert.

Convert this Informatica XML ETL workflow into canonical metadata JSON.

Rules:
1. Extract source schema
2. Identify transformations
3. Preserve pipeline order
4. Extract transformation logic
5. Identify target loader
6. Return ONLY valid JSON

Output format:

{{
 "workflow_name":"",
 "extractor": {{
   "name":"",
   "schema":[{{"name":"","datatype":""}}]
 }},
 "transformers":[
   {{
     "name":"",
     "type":"",
     "logic":""
   }}
 ],
 "loader": {{
   "location":""
   "type":""
 }}
}}

XML INPUT:
{xml_text}
"""

    response = llm.invoke([HumanMessage(content=prompt)])

    return response.content