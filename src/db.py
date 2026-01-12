# src/db.py
import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine


def get_engine(env_path: str | None = None):
    """
    Returns a SQLAlchemy engine using values from .env

    Expected keys:
      PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE
    """
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()  # looks for .env in current working dir

    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE")

    missing = [k for k, v in {
        "PGUSER": user,
        "PGPASSWORD": password,
        "PGDATABASE": db,
    }.items() if not v]

    if missing:
        raise ValueError(f"Missing .env values: {', '.join(missing)}")

    url = (
        f"postgresql+psycopg2://{user}:{quote_plus(password)}"
        f"@{host}:{port}/{db}"
    )
    return create_engine(url, pool_pre_ping=True)
