import os
import pymysql
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> pymysql.Connection:
    "Connect to the Grapher database."
    return pymysql.connect(
        db=os.getenv("GRAPHER_DB_NAME"),
        host=os.getenv("GRAPHER_DB_HOST"),
        port=int(os.getenv("GRAPHER_DB_PORT")),  # type: ignore
        user=os.getenv("GRAPHER_DB_USER"),
        password=os.getenv("GRAPHER_DB_PASS"),
        charset="utf8mb4",
        autocommit=True,
    )
