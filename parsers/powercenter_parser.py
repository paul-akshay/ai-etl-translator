from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from dotenv import load_dotenv

load_dotenv()

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0
)


def pc_to_metadata(xml_text: str):

    prompt = f"""
You are an expert in ETL migration and data engineering.

Your task is to convert an Informatica PowerCenter ETL mapping into a standardized canonical metadata model.

--------------------------------
CANONICAL METADATA MODEL
--------------------------------

Return JSON with this structure. No markdown. Only clean JSON.

{{
 "workflow_name": "",
 "extractor": {{
   "name": "",
   "path": "",
   "type": "",
   "schema": [
     {{
       "name": "",
       "datatype": ""
     }}
   ]
 }},
 "transformers": [
   {{
     "name": "",
     "type": "",
     "condition": "",
     "group_by": [],
     "aggregations": []
   }}
 ],
 "loader": {{
   "path": "",
   "type": ""
 }}
}}

----
EXTRACTOR
path can be set as 'file://input-<name>.csv'
type can be set as 'csv'
----

LOADER
path can be set as 'file://output-<name>.csv'
type can be set as 'csv'
----

--------------------------------
NORMALIZATION RULES
--------------------------------

1. Remove PowerCenter specific prefixes.

2. Normalize assignments such as:

   OUT_COL = IN_COL

into canonical column mappings.

3. Convert aggregation logic into structured format.

Example:

SUM(units_sold)

Canonical form:

"group_by":["product_id"],
"aggregations":[
  {{
    "function":"SUM",
    "column":"units_sold",
    "alias":"total_sales"
  }}
]

4. Filters must use:

"condition": "expression"

Example:

category = 'Electronics'

5. Preserve the transformation order defined in the XML mapping.

6. Extract transformation logic only when necessary.

7. Do NOT output PowerCenter syntax directly.

Instead convert it into canonical metadata fields.

--------------------------------
INPUT POWER CENTER XML
--------------------------------

{xml_text}

--------------------------------

Return ONLY valid JSON.
"""

    response = llm.invoke([HumanMessage(content=prompt)])

    return response.content.strip()