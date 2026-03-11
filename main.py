import os
import subprocess
from workflow import app

def push_to_github(file_path):
    subprocess.run(["git", "add", file_path])

    subprocess.run([
        "git",
        "commit",
        "-m",
        f"Add generated Spark ETL: {file_path}"
    ])

    subprocess.run(["git", "push", "origin", "master"])


with open("sample_mapping_informatica.xml") as f:
    xml_data = f.read()

result = app.invoke({
    "xml": xml_data
})

print("\nGenerated Canonical Metadata:\n")

print(result["metadata"])

print("\n=== Generated Spark Code ===\n")

spark_code = result["spark_code"]

spark_code = spark_code.replace("```scala", "").replace("```", "")


os.makedirs("generated-scala-etl", exist_ok=True)

file_path = "generated-scala-etl/sales_etl_trial.scala"

with open(file_path, "w") as f:
    f.write(spark_code)

print(f"Scala code saved to {file_path}")
#push_to_github(file_path)