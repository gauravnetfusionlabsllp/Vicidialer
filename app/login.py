from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pymysql

app = FastAPI()

def get_db():
    return pymysql.connect(
        host="192.168.15.177",
        user="cron",
        password="1234",
        database="asterisk",
        cursorclass=pymysql.cursors.DictCursor
    )

class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/login")
def login(data: LoginRequest):
    db = get_db()
    cursor = db.cursor()

    query = """
        SELECT user, full_name, active
        FROM vicidial_users
        WHERE user=%s AND pass=%s
        LIMIT 1
    """

    cursor.execute(query, (data.username, data.password))
    user = cursor.fetchone()

    cursor.close()
    db.close()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    if user["active"] != "Y":
        raise HTTPException(status_code=403, detail="User is inactive")

    return {
        "status": "success",
        "user": user["user"],
        "full_name": user["full_name"]
    }
