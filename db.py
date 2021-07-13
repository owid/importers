import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

# Connect to the database
def get_connection() -> pymysql.Connection:
    return pymysql.connect(
        db=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),  # type: ignore
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        charset="utf8mb4",
        autocommit=True,
    )
