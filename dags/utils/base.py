from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

engine = create_engine(
    "postgresql+psycopg2://airflow:airflow@postgres/postgres"
)

try:
    connection = engine.connect()
    check_database = connection.execute("SELECT datname FROM pg_database")
    existing_databases = [d for d in check_database]
    if (not "cloudmiddlewareassignment" in existing_databases):
        connection.execute("commit")
        connection.execute("create database cloudmiddlewareassignment")
        connection.close()
except:
    pass

engine = create_engine(
    "postgresql+psycopg2://airflow:airflow@postgres/cloudmiddlewareassignment"
)

session = Session(engine)
Base = declarative_base()