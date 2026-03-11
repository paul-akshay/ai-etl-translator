# AI ETL Translator

**Note: Created for POC. Models are not fully trained**

AI-powered tool that converts **legacy ETL workflows** (PowerCenter, Ab Initio) into a **canonical metadata model** and generates **modern ETL code**.

The project uses **LLM-assisted parsing + LangGraph workflows** to translate legacy ETL definitions


# Overview

The system performs two main tasks:

### 1️⃣ Metadata Extraction
Legacy ETL artifacts are parsed and converted into **canonical metadata**.

### 2️⃣ ETL Code Generation
The canonical metadata is used to generate **Spark Scala ETL code**.

---

# Supported Legacy ETL Frameworks

| Framework | Status |
|---|---|
PowerCenter | ✅ Minimal Support |
Ab Initio | ✅ Minimal Support |

---

# Architecture

The pipeline follows this flow:
Legacy ETL Artifacts -> Parser (LLM Assisted) -> Canonical Metadata -> Spark Code Generator


Internally this is implemented using **LangGraph workflows**.

---

# Workflow Design

Two workflows exist.

## Metadata Workflow

Parses legacy ETL (input/<source_framework>/<etl>) into canonical metadata (generated/metadata/<etl>).
Source ETL Input -> Parser Node -> Canonical Metadata

## ETL Generation Workflow

Uses canonical metadata to generate Spark ETL.
Metadata (generated/metadata/<etl>) -> Spark Generator Node -> Spark Scala Code (generated/spark-scala/<etl>).


---

# Development Setup

## 1️⃣ Clone Repository
```python
git clone https://github.com/paul-akshay/ai-etl-translator.git
cd ai-etl-translator
```

## 2️⃣ Create Virtual Environment
```python
python3 -m venv venv
```
Activate : 
```python
source venv/bin/activate
```
## 3️⃣ Install Dependencies
```python
pip install -r requirements.txt
```
## 4️⃣ Setup Environment Variables

Create a `.env` file.

```python
OPENAI_API_KEY=<your-api-key>
```
The project uses **LangChain OpenAI integration**.

---

# Configuration

Configuration is managed in:


Example:

```python
ETL_DIR = "hotel_booking"
PARSER_CHOSEN = POWERCENTER
```
# Running the Application
Start the CLI and choose the relevant option:
```python
python main.py
```

````
===================================
        AI - ETL Translator
===================================

1. Generate Metadata
2. Generate ETL
3. Exit

````

