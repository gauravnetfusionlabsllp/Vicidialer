from fastapi import FastAPI,HTTPException,UploadFile,File
import requests
import pandas as pd
import io 
from datetime import date,datetime, timedelta
from fastapi import Query, HTTPException
import mysql.connector

DB_CONFIG = {
    "host": "192.168.15.177",
    "user": "cron",
    "password": "1234",
    "database": "asterisk"
}

def normalize_phone(phone):
    return str(phone).strip()[-10:]

def load_existing_phones():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT phone_number FROM vicidial_list")
    phones = {normalize_phone(row[0]) for row in cursor.fetchall()}

    cursor.close()
    conn.close()
    return phones




app = FastAPI()

vicidial_url = "http://192.168.15.177:8068/vicidial/non_agent_api.php"

vici_user = 'AdminR'
Vici_pass = 'AdminR'
SOURCE = "FASTAPI"



@app.post("/upload_leads")
def upload_leads(payload : dict):
    required_fields = ['phone_number','list_id']

    for fields in required_fields:
        if fields not in payload:
            raise HTTPException(status_code=400,detail=f"{fields} is required")


    params = {
        "source": SOURCE,
        "user" : vici_user,
        "pass" : Vici_pass,
        "function" : "add_lead",
        "phone_number" : payload['phone_number'],
        "phone_code" : payload.get('phone_code',"1"),
        "list_id" : payload["list_id"],
        "first_name" : payload.get("first_name"),
        "last_name" : payload.get("last_name"),
        "campaign_id" : payload.get("campaign_id")
    }

    response = requests.get(vicidial_url,params=params,timeout=10)

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="vicidial API error")
    
    return {
        "status" : "success",
        "vicidial_response" : response.text
    }

@app.post("/upload_excel_leads")
def upload_excel_leads(file: UploadFile =File(...)):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400 , detail="only .xlsx file allowed")

    try:
        contents = file.file.read()
        df = pd.read_excel(io.BytesIO(contents))
        print('file read-----------------')
    except Exception as e:
        raise HTTPException(status_code=400 , detail=f"invalid excel file : {e}")
    
    required_columns = {"phone_number","list_id"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(
            status_code=400, detail=f"Excel must contain columns : {required_columns}"
        )
    
    existing_phones = load_existing_phones()

    success = 0
    failed = []


    for index,row in df.iterrows():
        phone = normalize_phone(row["phone_number"])

        if phone in existing_phones:
            failed.append({
                "row": index + 2,
                "phone": phone,
                "error": "Duplicate phone (already exists)"
            })
            continue
        params={
            "source": SOURCE,
            "user" : vici_user,
            "pass" : Vici_pass,
            "function" : "add_lead",
            "phone_number" : row['phone_number'],
            "phone_code" : "1",
            "list_id" : row["list_id"],
            "first_name" : str(row.get("first_name","")),
            "last_name" : str(row.get("last_name","")),
            "campaign_id" : str(row.get("campaign_id",""))
        }

        try: 
            response = requests.get(vicidial_url,params=params ,timeout=10)

            if "SUCCESS" in response.text.upper():
                success +=1 
            else:
                failed.append({
                    "row": index +2,
                    "phone": row["phone_number"],
                    "error" : response.text
                })

        except Exception as e: 
            failed.append({
                "row" : index+2,
                "phone" : row["phone_number"],
                "error" : str(e)
            })

    return {
        "total_records": len(df),
        "success": success,
        "failed": len(failed),
        "failed_details": failed
    }



@app.get("/leads")
def get_leads(
    page: int = 1,
    limit: int = 50,
    date: str | None = Query(default=None, description="YYYY-MM-DD")
):
    offset = (page - 1) * limit

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    base_query = """
        SELECT
            DATE(vl.entry_date) AS entry_date,
            vl.lead_id,
            vl.phone_number,
            vl.first_name,
            vl.last_name,
            vl.status,
            vl.list_id,
            vls.campaign_id
        FROM vicidial_list vl
        JOIN vicidial_lists vls
            ON vl.list_id = vls.list_id
    """

    params = []

    # ✅ Add date filter ONLY if date is provided
    if date:
        try:
            start_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        end_date = start_date + timedelta(days=1)

        base_query += """
            WHERE date(vl.entry_date) = %s
              AND date(vl.entry_date)  %s
        """
        params.extend([start_date, end_date])

    base_query += """
        ORDER BY vl.entry_date DESC
        LIMIT %s OFFSET %s
    """

    params.extend([limit, offset])

    cursor.execute(base_query, tuple(params))
    data = cursor.fetchall()

    cursor.close()
    conn.close()

    return {
        "page": page,
        "limit": limit,
        "filter_date": date,
        "count": len(data),
        "leads": data
    }

API_USER = "AdminR"
API_PASS = "AdminR"
AGENT_USER = "8999"


def call_number(phone):
    params = {
        "source": "crm",
        "user": API_USER,
        "pass": API_PASS,
        "agent_user": AGENT_USER,
        "function": "external_dial",
        "phone_code": "1",
        "value": phone,
        "preview": "NO",
        "search": "YES",
        "focus": "YES"
    }

    res = requests.get("http://192.168.15.177:8068/agc/api.php", params=params, timeout=10)

    print(res.text)
    return res.text



print("-------------------------------------------------")
app = FastAPI()

VICIDIAL_API_URL = "http://192.168.15.177:8068/agc/api.php"

API_USER = "AdminR"
API_PASS = "AdminR"
AGENT_USER = "8015"


@app.post("/call")
def call_number(phone: str):
    """
    Initiates an outbound call from VICIdial agent
    """

    params = {
        "source": "crm",
        "user": API_USER,
        "pass": API_PASS,
        "agent_user": AGENT_USER,
        "function": "external_dial",
        "phone_code": "1",      # change country code if needed
        "value": phone,
        "preview": "NO",
        "search": "YES",
        "focus": "YES"
    }

    try:
        response = requests.get(
            VICIDIAL_API_URL,
            params=params,
            timeout=10
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "status": "success",
        "vicidial_response": response.text
    }



@app.post("/hangup")
def hangup_call():
    params = {
        "source": "crm",
        "user": API_USER,
        "pass": API_PASS,
        "agent_user": AGENT_USER,
        "function": "external_hangup"
    }

    response = requests.get(VICIDIAL_API_URL, params=params, timeout=10)

    return {
        "status": "success",
        "vicidial_response": response.text
    }

