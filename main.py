"""
OmniBank — Account & Ledger Service (FastAPI + PostgreSQL)

Real project code:
- accounts table with NUMERIC balances
- transactions table as a real ledger
- atomic transfer endpoint
- JWT authentication
- RabbitMQ event publishing
"""
from sentence_transformers import SentenceTransformer
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import os
import time

import psycopg2
import psycopg2.extras
import pika
import json

# Semantic search model
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
# ---------------------------------------------------------------------
# Semantic search helper
# ---------------------------------------------------------------------

def generate_transaction_embedding(description):
    return embedding_model.encode(description).tolist()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------
# Auth config
# ---------------------------------------------------------------------

SECRET_KEY = "omnibank-dev-secret-change-this-later"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto"
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------

# Docker Compose provides these environment variables.
# The localhost defaults keep the project usable outside Docker.

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "omnibank"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------
# RabbitMQ
# ---------------------------------------------------------------------

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")


def publish_transfer_event(event):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST
        )
    )

    channel = connection.channel()

    channel.queue_declare(
        queue="transfer-requests",
        durable=True
    )

    channel.basic_publish(
        exchange="",
        routing_key="transfer-requests",
        body=json.dumps(event),
        properties=pika.BasicProperties(
            delivery_mode=2
        )
    )

    connection.close()


# ---------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------

def init_db():

    # PostgreSQL may take a few seconds to start inside Docker.
    # Retry the connection before giving up.

    max_retries = 15

    for attempt in range(max_retries):
        try:
            conn = get_conn()
            break
        except psycopg2.OperationalError:
            if attempt == max_retries - 1:
                raise

            print(
                f"PostgreSQL not ready yet. "
                f"Retrying ({attempt + 1}/{max_retries})..."
            )

            time.sleep(2)

    cur = conn.cursor()

    # Enable pgvector for semantic transaction search
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            balance NUMERIC(14,2) NOT NULL DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            from_account_id INTEGER REFERENCES accounts(id),
            to_account_id INTEGER REFERENCES accounts(id),
            amount NUMERIC(14,2) NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            description TEXT,
            embedding vector(384)
        )
    """)

    cur.execute("""
        ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS description TEXT DEFAULT ''
    """)

    cur.execute("""
        ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS embedding vector(384)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS transactions_embedding_hnsw
        ON transactions
        USING hnsw (embedding vector_cosine_ops)
    """)

    # Backfill embeddings for existing transactions
    cur.execute("""
        SELECT
            id,
            from_account_id,
            to_account_id,
            amount,
            status,
            description
        FROM transactions
        WHERE embedding IS NULL
    """)

    missing_transactions = cur.fetchall()

    for row in missing_transactions:
        transaction_id = row[0]
        from_account = row[1]
        to_account = row[2]
        amount = row[3]
        status = row[4]
        description = row[5] or ""

        text = (
            f"{status} transaction from account {from_account} "
            f"to account {to_account} "
            f"amount {amount}. "
            f"{description}"
        )

        embedding = generate_transaction_embedding(text)

        cur.execute(
            """
            UPDATE transactions
            SET embedding = %s
            WHERE id = %s
            """,
            (embedding, transaction_id)
        )

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staff_users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            hashed_password TEXT NOT NULL
        )
    """)

    conn.commit()

    # Seed default login
    cur.execute(
        "SELECT id FROM staff_users WHERE username = %s",
        ("admin",)
    )

    if cur.fetchone() is None:

        cur.execute(
            """
            INSERT INTO staff_users
            (username, hashed_password)
            VALUES (%s, %s)
            """,
            (
                "admin",
                pwd_context.hash("admin123")
            )
        )

        conn.commit()

    cur.close()
    conn.close()


# Initialize database when application starts
init_db()

# ---------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------


def create_access_token(data: dict):

    to_encode = data.copy()

    expire = datetime.utcnow() + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )

    to_encode.update({
        "exp": expire
    })

    return jwt.encode(
        to_encode,
        SECRET_KEY,
        algorithm=ALGORITHM
    )


def get_current_user(
    token: str = Depends(oauth2_scheme)
):

    credentials_error = HTTPException(
        status_code=401,
        detail="Invalid or expired token."
    )

    try:

        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )

        username = payload.get("sub")

        if username is None:
            raise credentials_error

    except JWTError:
        raise credentials_error

    return username


# ---------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------

class Account(BaseModel):
    name: str
    balance: float = 0.0


class TransferRequest(BaseModel):
    from_account_id: int
    to_account_id: int
    amount: float


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------

@app.get("/")
def read_root():

    return {
        "message": "OmniBank Account & Ledger Service is running."
    }


# ---------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------

@app.post("/login/")
def login(
    form_data: OAuth2PasswordRequestForm = Depends()
):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT hashed_password
        FROM staff_users
        WHERE username = %s
        """,
        (form_data.username,)
    )

    row = cur.fetchone()

    cur.close()
    conn.close()

    if row is None or not pwd_context.verify(
        form_data.password,
        row[0]
    ):
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password."
        )

    token = create_access_token({
        "sub": form_data.username
    })

    return {
        "access_token": token,
        "token_type": "bearer"
    }


# ---------------------------------------------------------------------
# Create account
# ---------------------------------------------------------------------

@app.post("/accounts/")
def create_account(
    account: Account,
    current_user: str = Depends(get_current_user)
):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO accounts (name, balance)
        VALUES (%s, %s)
        RETURNING id
        """,
        (
            account.name,
            account.balance
        )
    )

    new_id = cur.fetchone()[0]

    conn.commit()

    cur.close()
    conn.close()

    return {
        "message": f"Account created for {account.name}",
        "id": new_id
    }


# ---------------------------------------------------------------------
# Get accounts
# ---------------------------------------------------------------------

@app.get("/users/")
def get_users():
    print(">>> USERS 1: route entered", flush=True)

    conn = get_conn()
    print(">>> USERS 2: database connected", flush=True)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    print(">>> USERS 3: cursor created", flush=True)

    cur.execute("""
        SELECT id, name, balance
        FROM accounts
        ORDER BY id
    """)
    print(">>> USERS 4: query finished", flush=True)

    accounts = cur.fetchall()
    print(">>> USERS 5: fetched", accounts, flush=True)

    cur.close()
    conn.close()
    print(">>> USERS 6: connection closed", flush=True)

    for account in accounts:
        account["balance"] = float(account["balance"])

    print(">>> USERS 7: returning", flush=True)

    return {"accounts": accounts}

# ---------------------------------------------------------------------
# Get transactions
# ---------------------------------------------------------------------


@app.get("/transactions/")
def get_transactions():

    conn = get_conn()

    cur = conn.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    )

    cur.execute(
        """
        SELECT
            id,
            from_account_id,
            to_account_id,
            amount,
            status,
            created_at
        FROM transactions
        ORDER BY id DESC
        """
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    for row in rows:

        row["amount"] = float(row["amount"])
        row["created_at"] = row["created_at"].isoformat()

    return {
        "transactions": rows
    }


def generate_transaction_embedding(text):
    return embedding_model.encode(text).tolist()


@app.get("/search/")
def search_transactions(q: str):
    conn = get_conn()
    cur = conn.cursor()

    try:
        query_embedding = generate_transaction_embedding(q)

        cur.execute(
            """
            SELECT
                id,
                from_account_id,
                to_account_id,
                amount,
                status,
                created_at,
                description
            FROM transactions
            WHERE embedding IS NOT NULL
            ORDER BY embedding <-> %s::vector
            LIMIT 10
            """,
            (query_embedding,)
        )

        rows = cur.fetchall()

        return {
            "query": q,
            "results": [
                {
                    "id": row[0],
                    "from_account_id": row[1],
                    "to_account_id": row[2],
                    "amount": float(row[3]),
                    "status": row[4],
                    "created_at": row[5],
                    "description": row[6]
                }
                for row in rows
            ]
        }

    finally:
        cur.close()
        conn.close()
# ---------------------------------------------------------------------
# Transfer
# ---------------------------------------------------------------------


@app.post("/transfer/")
def transfer(
    req: TransferRequest,
    current_user: str = Depends(get_current_user)
):
    """
    Core ACID transfer:

    1. Lock sender account
    2. Check balance
    3. Check recipient
    4. Debit sender
    5. Credit recipient
    6. Record transaction
    7. Commit everything together
    """

    if req.amount <= 0:
        raise HTTPException(
            status_code=400,
            detail="Transfer amount must be positive."
        )

    if req.from_account_id == req.to_account_id:
        raise HTTPException(
            status_code=400,
            detail="Cannot transfer to the same account."
        )

    conn = get_conn()
    cur = None

    try:
        cur = conn.cursor()

        # -------------------------------------------------------------
        # Lock sender row
        # -------------------------------------------------------------

        cur.execute(
            """
            SELECT balance
            FROM accounts
            WHERE id = %s
            FOR UPDATE
            """,
            (req.from_account_id,)
        )

        row = cur.fetchone()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail="Sender account not found."
            )

        sender_balance = row[0]

        # -------------------------------------------------------------
        # Check sufficient funds
        # -------------------------------------------------------------

        if sender_balance < req.amount:

            # Record failed transaction in PostgreSQL
            cur.execute(
                """
                INSERT INTO transactions
                (
                    from_account_id,
                    to_account_id,
                    amount,
                    status
                )
                VALUES (%s, %s, %s, 'FAILED_INSUFFICIENT_FUNDS')
                """,
                (
                    req.from_account_id,
                    req.to_account_id,
                    req.amount
                )
            )

            # Commit the failed transaction record
            conn.commit()

            # Publish failed transfer event to RabbitMQ
            publish_transfer_event({
                "from_account_id": req.from_account_id,
                "to_account_id": req.to_account_id,
                "amount": req.amount,
                "status": "FAILED",
                "timestamp": datetime.utcnow().isoformat()
            })

            # Return error to frontend
            raise HTTPException(
                status_code=400,
                detail="Insufficient funds."
            )

        # -------------------------------------------------------------
        # Check recipient
        # -------------------------------------------------------------

        cur.execute(
            """
            SELECT id
            FROM accounts
            WHERE id = %s
            """,
            (req.to_account_id,)
        )

        if cur.fetchone() is None:
            raise HTTPException(
                status_code=404,
                detail="Recipient account not found."
            )

        # -------------------------------------------------------------
        # Debit sender
        # -------------------------------------------------------------

        cur.execute(
            """
            UPDATE accounts
            SET balance = balance - %s
            WHERE id = %s
            """,
            (
                req.amount,
                req.from_account_id
            )
        )

        # -------------------------------------------------------------
        # Credit recipient
        # -------------------------------------------------------------

        cur.execute(
            """
            UPDATE accounts
            SET balance = balance + %s
            WHERE id = %s
            """,
            (
                req.amount,
                req.to_account_id
            )
        )

        # -------------------------------------------------------------
        # Record successful transaction
        # -------------------------------------------------------------
        description = (
            f"Transfer of ₹{req.amount} "
            f"from account {req.from_account_id} "
            f"to account {req.to_account_id}"
        )

        embedding = generate_transaction_embedding(description)

        cur.execute(
            """
            INSERT INTO transactions
            (
                from_account_id,
                to_account_id,
                amount,
                status,
                description,
                embedding
            )
            VALUES (%s, %s, %s, 'SUCCESS', %s, %s)
            """,
            (
                req.from_account_id,
                req.to_account_id,
                req.amount,
                description,
                embedding
            )
        )

        # -------------------------------------------------------------
        # Commit database transaction
        # -------------------------------------------------------------

        conn.commit()

        # -------------------------------------------------------------
        # Publish successful transfer event to RabbitMQ
        # -------------------------------------------------------------

        publish_transfer_event({
            "from_account_id": req.from_account_id,
            "to_account_id": req.to_account_id,
            "amount": req.amount,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat()
        })

        return {
            "message":
                f"Transferred {req.amount} "
                f"from account {req.from_account_id} "
                f"to {req.to_account_id}."
        }

    except HTTPException:
        conn.rollback()
        raise

    except Exception as e:
        conn.rollback()

        raise HTTPException(
            status_code=500,
            detail=f"Transfer failed and was rolled back: {e}"
        )

    finally:
        if cur is not None:
            cur.close()

        conn.close()
