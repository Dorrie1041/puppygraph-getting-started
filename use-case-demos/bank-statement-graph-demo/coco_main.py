import base64
import json
import mimetypes
import os
import io
from dataclasses import dataclass
from typing import Any

import cocoindex
from openai import OpenAI
from pypdf import PdfReader
import hashlib
from decimal import Decimal, InvalidOperation
from psycopg.rows import dict_row
import psycopg
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Transaction:
    date_processed: str
    date_of_transaction: str
    card_id: str
    details: str
    withdrawals: str
    deposits: str
    balance: str


@dataclass
class BankStatement:
    statement_number: str
    account_number: str
    statement_period: str
    statement_date: str
    page_no: str
    account_holder: str
    business_name: str
    address: str
    opening_balance: str
    total_withdrawals: str
    total_deposits: str
    closing_balance: str
    transactions: list[Transaction]

def stable_id(*parts:Any) -> str:
    raw = "||".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() == "N/A":
        return None
    return text

def parse_money(value: Any) -> Decimal | None:
    text = clean_text(value)
    if text is None:
        return None
    text = text.replace("$", "").replace(",", "").strip()
    if not text:
        return None
    try: 
        return Decimal(text)
    except InvalidOperation:
        return None

class ExtractBankStatement(cocoindex.op.FunctionSpec):
    pass


@cocoindex.op.executor_class(cache=True, behavior_version=1)
class ExtractBankStatementExecutor:
    spec: ExtractBankStatement

    def prepare(self):
        self.client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    def _guess_mime(self, filename: str) -> str:
        mime, _ = mimetypes.guess_type(filename)
        return mime or "application/octet-stream"

    def _extract_pdf_text(self, content: bytes) -> str:
        reader = PdfReader(io.BytesIO(content))
        return "\n".join(page.extract_text() or "" for page in reader.pages)

    def _schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "statement_number": {"type": "string"},
                "account_number": {"type": "string"},
                "statement_period": {"type": "string"},
                "statement_date": {"type": "string"},
                "page_no": {"type": "string"},
                "account_holder": {"type": "string"},
                "business_name": {"type": "string"},
                "address": {"type": "string"},
                "opening_balance": {"type": "string"},
                "total_withdrawals": {"type": "string"},
                "total_deposits": {"type": "string"},
                "closing_balance": {"type": "string"},
                "transactions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "date_processed": {"type": "string"},
                            "date_of_transaction": {"type": "string"},
                            "card_id": {"type": "string"},
                            "details": {"type": "string"},
                            "withdrawals": {"type": "string"},
                            "deposits": {"type": "string"},
                            "balance": {"type": "string"},
                        },
                        "required": [
                            "date_processed",
                            "date_of_transaction",
                            "card_id",
                            "details",
                            "withdrawals",
                            "deposits",
                            "balance",
                        ],
                    },
                },
            },
            "required": [
                "statement_number",
                "account_number",
                "statement_period",
                "statement_date",
                "page_no",
                "account_holder",
                "business_name",
                "address",
                "opening_balance",
                "total_withdrawals",
                "total_deposits",
                "closing_balance",
                "transactions",
            ],
        }
    

    def __call__(self, content: bytes, filename: str) -> cocoindex.Json:
        mime = self._guess_mime(filename)

        instructions = """
        CRITICAL TRANSACTION EXTRACTION RULES:

        You must extract EVERY transaction row from the statement.
        Do NOT return only the first transaction.
        Do NOT return only one example transaction.
        Do NOT summarize transaction history.
        The "transactions" array must contain all transaction rows visible in the PDF.

        If a transaction table spans multiple pages, extract rows from every page.
        If rows have multi-line descriptions, merge the lines into one transaction object.
        If deposit and withdrawal columns both exist, preserve both columns exactly.
        If a row is a deposit/credit, the deposits field must contain the amount.
        If a row is a withdrawal/debit, the withdrawals field must contain the amount.
        

        You are extracting structured data from a bank statement.

        STRICT REQUIREMENTS:

        1. Extract ONLY real bank transaction rows from the main account activity section.
        Ignore:
        - policy text
        - overdraft summary
        - monthly service fee summary sections that are not transaction rows
        - worksheet pages
        - marketing text
        - explanations or legends

        2. Extract EVERY transaction row.
        - Do NOT skip any rows.
        - Include checks, withdrawals, deposits, transfers, fees, purchases, returns, ACH items, wire items, and branch/store withdrawals.

        3. Handle multi-line rows:
        - Some transactions are split across multiple lines.
        - Merge them into ONE transaction.

        4. Preserve order exactly as in the document.

        5. Field rules:
        - If amount is a withdrawal, put it in "withdrawals"
        - If amount is a deposit, put it in "deposits"
        - Do NOT put both
        - If a value is missing or unclear, use empty string ""

        6. DO NOT hallucinate:
        - Do not invent transactions
        - Do not infer hidden rows

        7. Return ONLY valid JSON matching the schema.
        """

        if mime == "application/pdf":
            extracted_text = self._extract_pdf_text(content)

            response = self.client.responses.create(
                model="gpt-4o",
                input=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": instructions},
                            {
                                "type": "input_text",
                                "text": extracted_text,
                            },
                        ],
                    }
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "bank_statement",
                        "schema": self._schema(),
                    }
                },
            )
        elif mime in {"image/png", "image/jpeg", "image/jpg", "image/webp"}:
            b64 = base64.b64encode(content).decode("utf-8")

            response = self.client.responses.create(
                model="gpt-4o",
                input=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": instructions},
                            {
                                "type": "input_image",
                                "image_url": f"data:{mime};base64,{b64}",
                            },
                        ],
                    }
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "bank_statement",
                        "schema": self._schema(),
                    }
                },
            )
        else:
            raise ValueError(f"Unsupported file type for {filename}: {mime}")

        raw = response.output_text
        if not raw or not raw.strip():
            raise ValueError(f"Model returned empty output for {filename}. Full response: {response}")

        return json.loads(raw)


class AggregateJsonFileTarget(cocoindex.op.TargetSpec):
    output_file: str


@cocoindex.op.target_connector(spec_cls=AggregateJsonFileTarget)
class AggregateJsonFileTargetConnector:
    @staticmethod
    def get_persistent_key(spec: AggregateJsonFileTarget, target_name: str) -> str:
        return spec.output_file

    @staticmethod
    def describe(key: str) -> str:
        return f"Local aggregate JSON file: {key}"

    @staticmethod
    def apply_setup_change(
        key: str,
        previous: AggregateJsonFileTarget | None,
        current: AggregateJsonFileTarget | None,
    ) -> None:
        if current is not None:
            out_dir = os.path.dirname(current.output_file)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

    @staticmethod
    def mutate(
        *all_mutations: tuple[AggregateJsonFileTarget, dict[str, dict[str, Any] | None]],
    ) -> None:
        for target, mutations in all_mutations:
            out_dir = os.path.dirname(target.output_file)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            
            existing: dict[str, Any] = {}

            if os.path.exists(target.output_file):
                with open(target.output_file, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                        if isinstance(data, list):
                            for row in data:
                                if isinstance(row, dict) and "filename" in row:
                                    existing[row["filename"]] = row
                    except json.JSONDecodeError:
                        existing = {}
                          
            for data_key, row in mutations.items():
                key_str = str(data_key)

                if row is None:
                    existing.pop(key_str, None)
                else:
                    row_with_key = dict(row)
                    row_with_key["filename"] = key_str
                    existing[key_str] = row_with_key   

            merged_rows = [existing[k] for k in sorted(existing.keys())]

            with open(target.output_file, "w", encoding="utf-8") as f:
                json.dump(merged_rows, f, indent=2, ensure_ascii=False)

class PostgresBankStatementTarget(cocoindex.op.TargetSpec):
    dsn: str
    schema: str = "public"


@cocoindex.op.target_connector(spec_cls=PostgresBankStatementTarget)
class PostgresBankStatementTargetConnector:
    @staticmethod
    def get_persistent_key(spec: PostgresBankStatementTarget, target_name: str) -> str:
        return f"{spec.dsn}::{spec.schema}"

    @staticmethod
    def describe(key: str) -> str:
        return f"PostgreSQL target: {key}"

    @staticmethod
    def apply_setup_change(
        key: str,
        previous: PostgresBankStatementTarget | None,
        current: PostgresBankStatementTarget | None,
    ) -> None:
        if current is None:
            return

        with psycopg.connect(current.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {current.schema};")

                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {current.schema}.accounts (
                        account_id TEXT PRIMARY KEY,
                        account_number TEXT,
                        account_holder TEXT,
                        business_name TEXT,
                        address TEXT,
                        raw_json JSONB NOT NULL
                    );
                    """
                )

                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {current.schema}.statements (
                        statement_id TEXT PRIMARY KEY,
                        account_id TEXT NOT NULL,
                        filename TEXT NOT NULL UNIQUE,
                        statement_number TEXT,
                        statement_period TEXT,
                        statement_date TEXT,
                        page_no TEXT,
                        opening_balance NUMERIC,
                        total_withdrawals NUMERIC,
                        total_deposits NUMERIC,
                        closing_balance NUMERIC,
                        raw_json JSONB NOT NULL,
                        FOREIGN KEY (account_id)
                            REFERENCES {current.schema}.accounts(account_id)
                            ON DELETE CASCADE
                    );
                    """
                )

                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {current.schema}.transactions (
                        transaction_id TEXT PRIMARY KEY,
                        statement_id TEXT NOT NULL,
                        row_index INT NOT NULL,
                        date_processed TEXT,
                        date_of_transaction TEXT,
                        card_id TEXT,
                        details TEXT,
                        withdrawals NUMERIC,
                        deposits NUMERIC,
                        balance NUMERIC,
                        raw_json JSONB NOT NULL,
                        FOREIGN KEY (statement_id)
                            REFERENCES {current.schema}.statements(statement_id)
                            ON DELETE CASCADE
                    );
                    """
                )

            conn.commit()

    @staticmethod
    def _upsert_statement(
        cur,
        schema: str,
        filename: str,
        row: dict[str, Any],
    ) -> None:
        statement = row.get("statement", {})

        account_number = clean_text(statement.get("account_number"))
        account_holder = clean_text(statement.get("account_holder"))
        business_name = clean_text(statement.get("business_name"))
        address = clean_text(statement.get("address"))

        account_id = stable_id(account_number, account_holder, business_name, address)

        statement_number = clean_text(statement.get("statement_number"))
        statement_period = clean_text(statement.get("statement_period"))
        statement_date = clean_text(statement.get("statement_date"))
        page_no = clean_text(statement.get("page_no"))

        statement_id = stable_id(
            filename,
            account_number,
            statement_number,
            statement_period,
            statement_date,
        )

        cur.execute(
            f"""
            INSERT INTO {schema}.accounts (
                account_id,
                account_number,
                account_holder,
                business_name,
                address,
                raw_json
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (account_id) DO UPDATE SET
                account_number = EXCLUDED.account_number,
                account_holder = EXCLUDED.account_holder,
                business_name = EXCLUDED.business_name,
                address = EXCLUDED.address,
                raw_json = EXCLUDED.raw_json;
            """,
            (
                account_id,
                account_number,
                account_holder,
                business_name,
                address,
                json.dumps(
                    {
                        "account_number": account_number,
                        "account_holder": account_holder,
                        "business_name": business_name,
                        "address": address,
                    },
                    ensure_ascii=False,
                ),
            ),
        )

        cur.execute(
            f"""
            INSERT INTO {schema}.statements (
                statement_id,
                account_id,
                filename,
                statement_number,
                statement_period,
                statement_date,
                page_no,
                opening_balance,
                total_withdrawals,
                total_deposits,
                closing_balance,
                raw_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (statement_id) DO UPDATE SET
                account_id = EXCLUDED.account_id,
                filename = EXCLUDED.filename,
                statement_number = EXCLUDED.statement_number,
                statement_period = EXCLUDED.statement_period,
                statement_date = EXCLUDED.statement_date,
                page_no = EXCLUDED.page_no,
                opening_balance = EXCLUDED.opening_balance,
                total_withdrawals = EXCLUDED.total_withdrawals,
                total_deposits = EXCLUDED.total_deposits,
                closing_balance = EXCLUDED.closing_balance,
                raw_json = EXCLUDED.raw_json;
            """,
            (
                statement_id,
                account_id,
                filename,
                statement_number,
                statement_period,
                statement_date,
                page_no,
                parse_money(statement.get("opening_balance")),
                parse_money(statement.get("total_withdrawals")),
                parse_money(statement.get("total_deposits")),
                parse_money(statement.get("closing_balance")),
                json.dumps(statement, ensure_ascii=False),
            ),
        )

        cur.execute(
            f"DELETE FROM {schema}.transactions WHERE statement_id = %s;",
            (statement_id,),
        )

        transactions = statement.get("transactions", [])
        print("DB insert:", filename, "transactions:", len(transactions))
        for idx, tx in enumerate(transactions):
            transaction_id = stable_id(
                statement_id,
                idx,
                tx.get("date_processed"),
                tx.get("date_of_transaction"),
                tx.get("details"),
                tx.get("withdrawals"),
                tx.get("deposits"),
                tx.get("balance"),
                tx.get("card_id"),
            )

            cur.execute(
                f"""
                INSERT INTO {schema}.transactions (
                    transaction_id,
                    statement_id,
                    account_id,
                    filename,
                    row_index,
                    date_processed,
                    date_of_transaction,
                    card_id,
                    details,
                    withdrawals,
                    deposits,
                    balance,
                    raw_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
                """,
                (
                    transaction_id,
                    statement_id,
                    account_id,
                    filename,
                    idx,
                    clean_text(tx.get("date_processed")),
                    clean_text(tx.get("date_of_transaction")),
                    clean_text(tx.get("card_id")),
                    clean_text(tx.get("details")),
                    parse_money(tx.get("withdrawals")),
                    parse_money(tx.get("deposits")),
                    parse_money(tx.get("balance")),
                    json.dumps(tx, ensure_ascii=False),
                ),
            )

    @staticmethod
    def mutate(
        *all_mutations: tuple[PostgresBankStatementTarget, dict[str, dict[str, Any] | None]],
    ) -> None:
        for target, mutations in all_mutations:
            with psycopg.connect(target.dsn, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    for data_key, row in mutations.items():
                        filename = str(data_key)

                        if row is None:
                            cur.execute(
                                f"DELETE FROM {target.schema}.statements WHERE filename = %s;",
                                (filename,),
                            )
                        else:
                            PostgresBankStatementTargetConnector._upsert_statement(
                                cur=cur,
                                schema=target.schema,
                                filename=filename,
                                row=row,
                            )

                conn.commit()



@cocoindex.flow_def(name="bank_statement_flow_v2")
def build_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="bank_data", binary=True)
    )

    statements = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["statement"] = doc["content"].transform(
            ExtractBankStatement(),
            filename=doc["filename"],
        )

        statements.collect(
            filename=doc["filename"],
            statement=doc["statement"],
        )

    statements.export(
        "bank_statement_json",
        AggregateJsonFileTarget(output_file="json_output/all_statements.json"),
        primary_key_fields=["filename"],
    )

    statements.export(
        "bank_statement_postgres",
        PostgresBankStatementTarget(
            dsn=os.environ["POSTGRES_DSN"],
            schema=os.environ.get("POSTGRES_SCHEMA", "public"),  
        ),
        primary_key_fields=["filename"],
    )


def main():
    cocoindex.init()
    build_flow.setup(report_to_stdout=True)
    build_flow.update(print_stats=True, reexport_targets=True)


if __name__ == "__main__":
    main()