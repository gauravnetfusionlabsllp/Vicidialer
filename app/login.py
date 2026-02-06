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




import asyncio
from datetime import datetime
from fastapi import FastAPI

AMI_HOST = "127.0.0.1"
AMI_PORT = 5038
AMI_USER = "fastapi"
AMI_SECRET = "supersecretpassword"

app = FastAPI()

calls = {}  # linkedid -> call state


def parse_ami_event(raw: str) -> dict:
    event = {}
    for line in raw.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            event[k.strip()] = v.strip()
    return event


def get_call(linkedid):
    if linkedid not in calls:
        calls[linkedid] = {
            "linkedid": linkedid,
            "dial_start": None,
            "ringing": None,
            "answered": None,
            "bridge": None,
            "hangup": None,
            "hangup_cause": None,
            "billsec": 0,
            "disposition": None,
        }
    return calls[linkedid]


# ============================
# EVENT HANDLERS
# ============================

def handle_dial_begin(e):
    call = get_call(e["Linkedid"])
    call["dial_start"] = datetime.utcnow()
    print("📤 DIAL BEGIN", e["Destination"])


def handle_ringing(e):
    call = get_call(e["Linkedid"])
    if not call["ringing"]:
        call["ringing"] = datetime.utcnow()
        print("📞 RINGING", e["Channel"])


def handle_answered(e):
    call = get_call(e["Linkedid"])
    if not call["answered"]:
        call["answered"] = datetime.utcnow()
        print("✅ ANSWERED", e["Channel"])


def handle_bridge(e):
    call = get_call(e["Linkedid"])
    if not call["bridge"]:
        call["bridge"] = datetime.utcnow()
        print("🔗 CONNECTED (BRIDGE)")


def handle_hangup(e):
    call = get_call(e["Linkedid"])
    call["hangup"] = datetime.utcnow()
    call["hangup_cause"] = e.get("Cause-txt")
    print("❌ HANGUP", call["hangup_cause"])


def handle_cdr(e):
    call = get_call(e["Linkedid"])
    call["billsec"] = int(e.get("Billsec", 0))
    call["disposition"] = e.get("Disposition")

    print("📊 CDR",
          "Disposition:", call["disposition"],
          "Billsec:", call["billsec"])

    # 🔥 At this point VICIdial writes final DB rows
    # You can safely persist & cleanup here


# ============================
# AMI LISTENER
# ============================

async def ami_listener():
    reader, writer = await asyncio.open_connection(AMI_HOST, AMI_PORT)

    writer.write(
        f"Action: Login\r\nUsername: {AMI_USER}\r\nSecret: {AMI_SECRET}\r\n\r\n".encode()
    )
    await writer.drain()

    buffer = ""

    while True:
        data = await reader.read(4096)
        if not data:
            break

        buffer += data.decode(errors="ignore")

        while "\r\n\r\n" in buffer:
            raw, buffer = buffer.split("\r\n\r\n", 1)
            e = parse_ami_event(raw)

            event = e.get("Event")
            linkedid = e.get("Linkedid")
            if not event or not linkedid:
                continue

            # -------- PRE-CALL --------
            if event == "DialBegin":
                handle_dial_begin(e)

            # -------- RINGING / PROGRESS --------
            elif event == "Newstate":
                state = e.get("ChannelStateDesc")
                if state == "Ringing":
                    handle_ringing(e)
                elif state == "Up":
                    handle_answered(e)

            # -------- ANSWER / CONNECT --------
            elif event == "DialEnd" and e.get("DialStatus") == "ANSWER":
                handle_answered(e)

            elif event == "BridgeEnter":
                handle_bridge(e)

            # -------- CALL END --------
            elif event == "Hangup":
                handle_hangup(e)

            elif event == "Cdr":
                handle_cdr(e)


@app.on_event("startup")
async def startup():
    asyncio.create_task(ami_listener())
