import os
from dotenv import load_dotenv

load_dotenv()

# DigitalOcean Spaces
DO_SPACES_KEY = os.environ["DO_SPACES_KEY"]
DO_SPACES_SECRET = os.environ["DO_SPACES_SECRET"]
DO_SPACES_ENDPOINT = os.environ["DO_SPACES_ENDPOINT"]
DO_SPACES_BUCKET = os.environ["DO_SPACES_BUCKET"]

# Postgres warehouse
PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]
PG_DATABASE = os.environ["PG_DATABASE"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_SSLMODE = os.environ.get("PG_SSLMODE", "require")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}?sslmode={PG_SSLMODE}"