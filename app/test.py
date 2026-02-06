import asyncio
from datetime import datetime

# ===============================
# AMI CONFIG
# ===============================
AMI_HOST = "192.168.15.165"
AMI_PORT = 5038
AMI_USER = "fastapi"
AMI_SECRET = "supersecretpassword"

# ===============================
# IN-MEMORY CALL CACHE
# ===============================
call_states = {}

# ===============================
# AMI EVENT PARSER
# ===============================
def parse_ami_event(raw: str) -> dict:
    event = {}
    for line in raw.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            event[k.strip()] = v.strip()
    return event

# ===============================
# CALL STATE
# ===============================
def get_call(linkedid):
    if linkedid not in call_states:
        call_states[linkedid] = {
            "linkedid": linkedid,
            "uniqueid": None,
            "callerid": None,
            "dial_start": None,
            "ringing": None,
            "answered": None,
            "bridge": None,
            "hangup": None,
            "hangup_cause": None,
            "billsec": 0,
            "disposition": None,
        }
    return call_states[linkedid]

# ===============================
# EVENT HANDLER
# ===============================
async def handle_event(e):
    event = e.get("Event")
    linkedid = e.get("Linkedid")

    if not linkedid:
        return

    call = get_call(linkedid)

    call["uniqueid"] = e.get("Uniqueid", call["uniqueid"])
    call["callerid"] = e.get("CallerIDNum", call["callerid"])

    if event == "DialBegin":
        call["dial_start"] = datetime.utcnow()
        print("📞 DIAL", linkedid)

    elif event == "Newstate":
        state = e.get("ChannelStateDesc")

        if state == "Ringing" and not call["ringing"]:
            call["ringing"] = datetime.utcnow()
            print("🔔 RINGING", linkedid)

        elif state == "Up" and not call["answered"]:
            call["answered"] = datetime.utcnow()
            print("✅ ANSWERED", linkedid)

    elif event == "BridgeEnter" and not call["bridge"]:
        call["bridge"] = datetime.utcnow()
        print("🔗 CONNECTED", linkedid)

    elif event == "Hangup":
        call["hangup"] = datetime.utcnow()
        call["hangup_cause"] = e.get("Cause-txt")
        print("❌ HANGUP", linkedid, call["hangup_cause"])

    elif event == "Cdr":
        call["billsec"] = int(e.get("Billsec", 0))
        call["disposition"] = e.get("Disposition")

        print("📊 CDR", linkedid, call["disposition"], call["billsec"])

        call_states.pop(linkedid, None)

# ===============================
# AMI KEEPALIVE
# ===============================
async def ami_keepalive(writer):
    try:
        while True:
            writer.write(b"Action: Ping\r\n\r\n")
            await writer.drain()
            await asyncio.sleep(30)
    except Exception:
        print("⚠️ Keepalive stopped")

# ===============================
# AMI LISTENER
# ===============================
async def ami_listener():
    print("🔌 Connecting to AMI...")
    reader, writer = await asyncio.open_connection(AMI_HOST, AMI_PORT)

    writer.write(
        f"Action: Login\r\n"
        f"Username: {AMI_USER}\r\n"
        f"Secret: {AMI_SECRET}\r\n\r\n".encode()
    )
    await writer.drain()

    asyncio.create_task(ami_keepalive(writer))

    buffer = ""

    while True:
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=60)
        except asyncio.TimeoutError:
            print("⏳ Read timeout, pinging AMI")
            writer.write(b"Action: Ping\r\n\r\n")
            await writer.drain()
            continue

        if not data:
            raise ConnectionError("AMI connection closed")

        buffer += data.decode(errors="ignore")

        while "\r\n\r\n" in buffer:
            raw, buffer = buffer.split("\r\n\r\n", 1)
            event = parse_ami_event(raw)
            await handle_event(event)

# ===============================
# MAIN (AUTO RECONNECT)
# ===============================
async def main():
    print("✅ AMI Call Monitor Started")

    while True:
        try:
            await ami_listener()
        except Exception as e:
            print("🔌 AMI disconnected:", e)
            await asyncio.sleep(5)

# ===============================
# ENTRY POINT
# ===============================
if __name__ == "__main__":
    asyncio.run(main())
