# ═══════════════════════════════════════════════════════════════
#  dependencies.py
#  Shared helpers used by both Correct_demo_code.py and email_routes.py
#  Import from HERE — never cross-import between the two main files.
# ═══════════════════════════════════════════════════════════════

import os
import psycopg2
import pymysql
import pymysql.cursors
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# AUTH CONFIG  (single source of truth)
# ─────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM  = "HS256"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# ─────────────────────────────────────────────
# DB CONFIG
# ─────────────────────────────────────────────
MYSQL_DB = {
    "host":     "192.168.15.165",
    "port":     3306,
    "user":     "cron",
    "password": "1234",
    "database": "asterisk",
}

POSTGRES_DB = {
    "host":     "192.168.15.105",
    "port":     5432,
    "user":     "postgres",
    "password": "Soft!@7890",
    "database": "customDialer",
}

# ─────────────────────────────────────────────
# DB CONNECTION HELPERS
# ─────────────────────────────────────────────
def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_DB["host"], port=MYSQL_DB["port"],
        user=MYSQL_DB["user"], password=MYSQL_DB["password"],
        database=MYSQL_DB["database"],
        cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4"
    )

def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_DB["host"], port=POSTGRES_DB["port"],
        user=POSTGRES_DB["user"], password=POSTGRES_DB["password"],
        dbname=POSTGRES_DB["database"]
    )

# ─────────────────────────────────────────────
# AUTH HELPER
# ─────────────────────────────────────────────
def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload       = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username      = payload.get("sub")
        is_admin      = payload.get("isAdmin", False)
        user_level    = payload.get("user_level", 1)
        campaign_name = payload.get("campaign_name")
        campaign_id   = payload.get("campaign_id")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {
            "username":      username,
            "isAdmin":       is_admin,
            "user_level":    user_level,
            "campaign_name": campaign_name,
            "campaign_id":   campaign_id,
        }
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired or invalid"
        )
