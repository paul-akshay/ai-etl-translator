import os
import json
from typing import Any

from workflow import metadata_app, etl_app
from config import (
    ETL_DIR,
    PARSER_CHOSEN
)

from constants import (
    AB_INITIO,
    POWERCENTER
)


# --------------------------------
# FILE PATHS
# --------------------------------
INFORMATICA_XML = f"input/powercenter/{ETL_DIR}/mapping.xml"

ABINITIO_GRAPH = f"input/abinitio/{ETL_DIR}/graph.mp"
ABINITIO_SCHEMA = f"input/abinitio/{ETL_DIR}/schema.dml"
ABINITIO_TRANSFORM = f"input/abinitio/{ETL_DIR}/transform.xfr"

OUTPUT_ETL_FILE_PATH = f"generated/etl/spark-scala/{ETL_DIR}"
OUTPUT_MD_FILE_PATH = f"generated/metadata/{ETL_DIR}"


# --------------------------------
# WRITE FUNCTIONS
# --------------------------------
def write_md(metadata: Any):

    os.makedirs(OUTPUT_MD_FILE_PATH, exist_ok=True)

    file_path = os.path.join(OUTPUT_MD_FILE_PATH, "metadata.json")

    metadata_sanitized = json.loads(metadata)

    with open(file_path, "w") as f:
        json.dump(metadata_sanitized, f, indent=2)

    print(f"\nMetadata generated. Location: {file_path}")


def write_etl_code(spark_code: str):

    os.makedirs(OUTPUT_ETL_FILE_PATH, exist_ok=True)

    file_path = os.path.join(OUTPUT_ETL_FILE_PATH, "spark_etl.scala")

    with open(file_path, "w") as f:
        f.write(spark_code)

    print(f"\nSpark Code Generated. Location: {file_path}")


# --------------------------------
# SOURCE READERS
# --------------------------------
def read_powercenter_source():

    if not os.path.exists(INFORMATICA_XML):
        raise Exception(f"PowerCenter mapping not found: {INFORMATICA_XML}")

    with open(INFORMATICA_XML, "r") as f:
        return f.read()


def read_abinitio_artifacts():

    if not os.path.exists(ABINITIO_GRAPH):
        raise Exception(f"Graph file not found: {ABINITIO_GRAPH}")

    if not os.path.exists(ABINITIO_SCHEMA):
        raise Exception(f"Schema file not found: {ABINITIO_SCHEMA}")

    if not os.path.exists(ABINITIO_TRANSFORM):
        raise Exception(f"Transform file not found: {ABINITIO_TRANSFORM}")

    with open(ABINITIO_GRAPH, "r") as f:
        graph = f.read()

    with open(ABINITIO_SCHEMA, "r") as f:
        schema = f.read()

    with open(ABINITIO_TRANSFORM, "r") as f:
        transform = f.read()

    return {
        "graph": graph,
        "schema": schema,
        "transform": transform
    }


# --------------------------------
# READ METADATA
# --------------------------------
def read_metadata():

    file_path = os.path.join(OUTPUT_MD_FILE_PATH, "metadata.json")

    if not os.path.exists(file_path):
        raise Exception(f"Metadata not found: {file_path}")

    with open(file_path, "r") as f:
        return json.load(f)


# --------------------------------
# OPTION 1 : GENERATE METADATA
# --------------------------------
def generate_metadata():

    print("Executing Workflow for Metadata Generation..")
    print(f"Parser Chosen : {PARSER_CHOSEN}")
    print(f"Source ETL Dir : {ETL_DIR}")

    if PARSER_CHOSEN == POWERCENTER:

        xml_data = read_powercenter_source()

        result = metadata_app.invoke({
            "xml": xml_data
        })

    elif PARSER_CHOSEN == AB_INITIO:

        abinitio_data = read_abinitio_artifacts()

        result = metadata_app.invoke({
            "graph": abinitio_data["graph"],
            "schema": abinitio_data["schema"],
            "transform": abinitio_data["transform"]
        })

    else:
        raise Exception("Invalid parser selected")

    metadata = result["metadata"]

    if not metadata or metadata.strip() == "":
        raise Exception("LLM returned empty metadata")

    write_md(metadata)


# --------------------------------
# OPTION 2 : GENERATE ETL
# --------------------------------
def generate_etl():

    print("Executing Workflow for ETL Generation..")

    metadata = read_metadata()

    result = etl_app.invoke({
        "metadata": metadata
    })

    spark_code = result["spark_code"]

    write_etl_code(spark_code)


# --------------------------------
# CLI MENU
# --------------------------------
def show_menu():

    while True:

        print("\n===================================")
        print("        AI - ETL Translator        ")
        print("===================================")

        print("1. Generate Metadata")
        print("2. Generate ETL")
        print("3. Exit")

        choice = input("\nSelect an option: ")

        if choice == "1":
            try:
                generate_metadata()
            except Exception as e:
                print("Error:", e)

        elif choice == "2":
            try:
                generate_etl()
            except Exception as e:
                print("Error:", e)

        elif choice == "3":
            print("\nExiting AI - ETL Translator. Goodbye!\n")
            break

        else:
            print("\nInvalid option. Please select 1, 2, or 3.")


# --------------------------------
# MAIN ENTRY
# --------------------------------
if __name__ == "__main__":
    show_menu()