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


## Quickstart

Create a.env file:

(1) Prepare environment variables

```
OPENAI_API_KEY=your_openai_key
POSTGRES_DSN=your_dsn_url
POSTGRES_SCHEMA=public
COCOINDEX_DATABASE_URL=your_database_url
```


(2) Add your PDF data
Place bank statement PDFs in:

```
bank_data/
```

(3) Run the data pipeline

```
python coco_main.py
```

This will:
	•	Extract structured data from PDFs
	•	Generate JSON output
	•	Insert data into PostgreSQL tables

Tables created:
	•	accounts
	•	statements
	•	transactions

## Deployment

Open PuppyGraph

Go to:

```
http://localhost:8081
```

Then

Upload your schema.json in

## Graph Model

```
(Account) ──OwnsStatement──> (Statement) ──HasTransaction──> (Transaction)
```

	•	Account → bank account holder
	•	Statement → PDF statement
	•	Transaction → individual transaction row



