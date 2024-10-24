from dotenv import load_dotenv
import os
import psycopg2

# Load environment variables from .env file
load_dotenv()

def connect_db():
    # Access environment variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    # Connect to the PostgreSQL database
    return psycopg2.connect(host=db_host, port=db_port,
                            database=db_name, user=db_user, password=db_password)

# Get all FDIC CNPJs from the database
def get_all_fdic_cnpj_from_db(conn):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Select all CNPJs from the fdic table
        cursor.execute("SELECT cnpj FROM fdic")

        # Fetch all rows from the result
        cnpjs = cursor.fetchall()

    finally:
        # Close the cursor
        cursor.close()

    return cnpjs

# Get all FDIC CNPJs from the database
def get_fdic_cnpj_not_filled_from_db(conn):
    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Select all CNPJs from the fdic table
        cursor.execute("SELECT cnpj FROM fdic WHERE name IS NULL")

        # Fetch all rows from the result
        cnpjs = cursor.fetchall()

    finally:
        # Close the cursor
        cursor.close()

    return cnpjs