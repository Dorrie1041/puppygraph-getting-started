# 📄 Bank Statement → Graph → Chatbot Demo

## Summary

This demo builds a financial transaction graph from bank statement PDFs to demonstrate how PuppyGraph can be applied to document-based financial data using existing relational databases.

The pipeline uses:
- PDF bank statements as input  
- CocoIndex for structured extraction  
- PostgreSQL for storage  
- PuppyGraph for graph modeling  
- A chatbot interface for natural-language querying  

The result is an interactive system where users can explore financial data without writing complex queries.

---

## 🧰 Prerequisites

- Python 3  
- pip (and optionally a virtual environment like `venv` or `conda`)  
- Docker  
- Docker Compose  

### Python packages

```bash
pip install cocoindex psycopg[binary] python-dotenv openai pypdf
```

---

## 🚀 Quickstart

### (1) Prepare environment variables

Create a `.env` file with the following variables:

```env
OPENAI_API_KEY=your_openai_key
POSTGRES_DSN=your_dsn_url
POSTGRES_SCHEMA=public
COCOINDEX_DATABASE_URL=your_database_url
```

### (2) Add your PDF data

Place bank statement PDFs in:

```
bank_data/
```

### (3) Run the data pipeline

```bash
python coco_main.py
```

This will:
- Extract structured data from PDFs
- Generate JSON output
- Insert data into PostgreSQL tables

**Tables created:**
- `accounts`
- `statements`
- `transactions`

---

## 🚀 Deployment

### Open PuppyGraph

Navigate to:

```
http://localhost:8081
```

Upload your `schema.json` file in the PuppyGraph interface.

---

## 📊 Graph Model

```
(Account) ──OwnsStatement──> (Statement) ──HasTransaction──> (Transaction)
```

**Entity Descriptions:**
- **Account** → Bank account holder
- **Statement** → PDF statement document
- **Transaction** → Individual transaction record
