from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from dotenv import load_dotenv

from parsers.abinitio_component_mappings import ABINITIO_COMPONENT_MAP

load_dotenv()

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0
)


def abinitio_to_metadata(graph_text: str, schema_text: str, transform_text: str):

    component_mapping_text = "\n".join(
        f"{k} -> {v}" for k, v in ABINITIO_COMPONENT_MAP.items()
    )

    etl_context = f"""
Graph Definition:
{graph_text}

Schema Definition:
{schema_text}

Transform Logic:
{transform_text}
"""

    prompt = f"""
You are an expert in ETL migration and data engineering.

Your task is to convert legacy ETL definitions into a standardized canonical metadata model.

The input may come from tools such as:
- Informatica PowerCenter
- Ab Initio
- Talend
- SSIS
- Other ETL frameworks

--------------------------------
CANONICAL METADATA MODEL
--------------------------------

Return JSON with this structure. No markdown. Just clean Json:

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
     "aggregations": [],
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
TRANSFORMATION TYPE MAPPINGS
--------------------------------

Use these canonical mappings whenever applicable:

{component_mapping_text}

--------------------------------
NORMALIZATION RULES
--------------------------------

1. Remove tool-specific prefixes such as:
   in.
   out.

2. Convert assignments like:

   out.col = in.col

   into canonical column mappings:

   "columns":[
     {{"source":"col","alias":"col"}}
   ]

3. Convert aggregation expressions into structured format.

Example input:

out.total_sales = sum(in.units_sold)

Canonical output:

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
No need to fill in other columns

Example:

category = 'Electronics'

5. Preserve transformation order as it appears in the pipeline.

6. Extract transformation logic only when needed.

7. Do NOT return tool-specific expressions like:

out.col = in.col

Instead normalize them.

--------------------------------
INPUT ETL
--------------------------------

{etl_context}

--------------------------------

Return ONLY valid JSON.
"""

    response = llm.invoke([HumanMessage(content=prompt)])

    return response.content.strip()