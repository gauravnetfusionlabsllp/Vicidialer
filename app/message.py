from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
import pymysql
import pymysql.cursors
import psycopg2
import psycopg2.extras
from psycopg2 import pool
import requests
from typing import Optional, List
from pydantic import BaseModel, validator
import os
import re
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

# ─────────────────────────────────────────────
# AUTH CONFIG
# ─────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM  = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# ─────────────────────────────────────────────
# MYSQL CONFIG (VICIdial)
# ─────────────────────────────────────────────
MYSQL_DB = {
    "host":     "192.168.15.165",
    "port":     3306,
    "user":     "cron",
    "password": "1234",
    "database": "asterisk",
}

# ─────────────────────────────────────────────
# POSTGRESQL CONFIG
# ─────────────────────────────────────────────
POSTGRES_DB = {
    "host":     "192.168.15.105",
    "port":     5432,
    "user":     "postgres",
    "password": "Soft!@7890",
    "database": "customDialer",
}

# ─────────────────────────────────────────────
# VICIDIAL API CONFIG
# ─────────────────────────────────────────────
VICIDIAL_API_URL = os.getenv("VICIDIAL_API_URL", "http://192.168.15.165:5165/agc/api.php")
API_USER         = os.getenv("VICIDIAL_USER", "AdminR")
API_PASS         = os.getenv("VICIDIAL_PASS", "AdminR")
SOURCE           = "FASTAPI"

# ─────────────────────────────────────────────
# BHASHSMS CONFIG — all from .env
# ─────────────────────────────────────────────
BHASH_USER        = os.getenv("BHASH_USER")
BHASH_PASS        = os.getenv("BHASH_PASS")
BHASH_SMS_URL     = os.getenv("BHASH_SMS_URL", "http://bhashsms.com/api/sendmsg.php")
BHASH_WA_URL      = os.getenv("BHASH_WA_URL",  "http://bhashsms.com/api/sendmsg.php")
BHASH_SENDER      = os.getenv("BHASH_SENDER")         # 6-char Sender ID e.g. SGFXIN
BHASH_WA_TEMPLATE = os.getenv("BHASH_WA_TEMPLATE")    # Approved WA template ID
BHASH_WA_NUMBER   = os.getenv("BUSINESS_NUMBER", "").replace("+", "").strip()       # e.g. 91XXXXXXXXXX

# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(title="VICIdial Dashboard", version="7.0.0")

ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://192.168.15.104:5000,http://localhost:5000,http://192.168.15.104:5500,http://localhost:5500"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# POSTGRESQL POOL
# ─────────────────────────────────────────────
pgsqlPool = pool.SimpleConnectionPool(
    minconn=1, maxconn=20,
    dbname=POSTGRES_DB["database"],
    user=POSTGRES_DB["user"],
    password=POSTGRES_DB["password"],
    host=POSTGRES_DB["host"],
    port=POSTGRES_DB["port"],
)


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

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

# FIX: centralised phone normalizer + basic validation
def normalize_phone(raw: str) -> str:
    """Strip spaces, dashes, and leading '+'. Raise 400 if not 7–15 digits."""
    phone = re.sub(r"[\s\-\(\)]", "", raw).lstrip("+")
    if not re.fullmatch(r"\d{7,15}", phone):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid phone number: '{raw}'. Must be 7–15 digits."
        )
    return phone


# ═══════════════════════════════════════════════════════════════
#  PYDANTIC MODELS
# ═══════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    username:      str
    password:      str
    campaign_id:   str
    campaign_name: str
    role:          str

class DeleteLeadRequest(BaseModel):
    phone_number: List[str]

class BhashMessageRequest(BaseModel):
    phone_number:   str
    client_name:    Optional[str] = ""
    custom_message: Optional[str] = None

    # FIX: validate phone at model level
    @validator("phone_number")
    def validate_phone(cls, v):
        cleaned = re.sub(r"[\s\-\(\)]", "", v).lstrip("+")
        if not re.fullmatch(r"\d{7,15}", cleaned):
            raise ValueError(f"Invalid phone number: '{v}'")
        return cleaned


# ═══════════════════════════════════════════════════════════════
#  BHASHSMS — SEND WHATSAPP
# ═══════════════════════════════════════════════════════════════

@app.post("/send-bhash-whatsapp")
def send_bhash_whatsapp(data: BhashMessageRequest):
    phone   = data.phone_number
    name    = data.client_name or "Valued Customer"
    # message = data.custom_message or (
    #     f"Hello {name}, thank you for your interest. "
    #     f"Our team will connect with you shortly."
    # )
    message = f"Your OTP is {{1}}. Do not share it with anyone."

    wa_params = {
        "user":        BHASH_USER,
        "password":    BHASH_PASS,
        "mobile":      phone,
        "msg":         message,
        "pass":        BHASH_WA_NUMBER,   # ← no + sign
        "type":        "WAP",
        "template_id": BHASH_WA_TEMPLATE, # ← sgfx_2712
    }

    try:
        resp = requests.get(BHASH_WA_URL, params=wa_params, timeout=10)

        print("=== BHASH DEBUG ===")
        print("URL CALLED   :", resp.url)
        print("STATUS CODE  :", resp.status_code)
        print("RESPONSE BODY:", repr(resp.text))
        print("===================")

        resp_text = resp.text.strip()

        if not resp_text:
            raise HTTPException(status_code=400, detail={
                "error":      "Still empty — template_id or WA number is wrong",
                "url_called": str(resp.url),
            })

        if any(kw in resp_text.lower() for kw in ("success", "submitted", "queued")):
            return {
                "status":         "success",
                "channel":        "whatsapp",
                "from_number":    BHASH_WA_NUMBER,
                "to":             phone,
                "message":        message,
                "bhash_response": resp_text,
            }

        raise HTTPException(status_code=400, detail={
            "error":          "BhashSMS rejected",
            "bhash_response": resp_text,
        })

    except HTTPException:
        raise
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="BhashSMS request timed out.")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Network error: {str(e)}")
    
# ═══════════════════════════════════════════════════════════════
#  BHASHSMS — SEND SMS
# ═══════════════════════════════════════════════════════════════

@app.post("/send-bhash-sms")
def send_bhash_sms(data: BhashMessageRequest):
    """
    Send a plain SMS via BhashSMS.
    Uses type="normal" for standard DLT-registered messages.
    """
    phone   = data.phone_number
    name    = data.client_name or "Valued Customer"
    message = data.custom_message or (
        f"Hello {name}, our team will contact you soon. "
        f"Thank you for your interest."
    )

    sms_params = {
        "user":     BHASH_USER,
        "password": BHASH_PASS,
        "mobile":   phone,
        "msg":      message,
        "pass":     BHASH_SENDER,
        "type":     "normal",
    }

    try:
        resp = requests.get(BHASH_SMS_URL, params=sms_params, timeout=10)
        resp.raise_for_status()
        return {
            "status":         "success",
            "channel":        "sms",
            "to":             phone,
            "message":        message,
            "bhash_response": resp.text.strip(),
        }
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="BhashSMS SMS request timed out.")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Network error: {str(e)}")


# ═══════════════════════════════════════════════════════════════
#  COMBINED — SEND BOTH (WhatsApp + SMS)
# ═══════════════════════════════════════════════════════════════

@app.post("/notify-client")
def notify_client(data: BhashMessageRequest):
    """
    Single endpoint: sends WhatsApp + SMS together.
    Both use the shared business number as sender.
    Individual failures are captured in results — the endpoint
    always returns 200 so the caller can inspect both outcomes.
    """
    phone   = data.phone_number
    name    = data.client_name or "Valued Customer"
    message = data.custom_message or (
        f"Hello {name}, thank you for your interest. "
        f"Our team will connect with you shortly."
    )

    results: dict = {}

    # ── 1. WhatsApp ───────────────────────────────────────────
    try:
        wa_resp = requests.get(BHASH_WA_URL, params={
            "user":        BHASH_USER,
            "password":    BHASH_PASS,
            "mobile":      phone,
            "msg":         message,
            "pass":        BHASH_SENDER,
            "type":        "WAP",            # ← corrected
            "template_id": BHASH_WA_TEMPLATE,
        }, timeout=10)
        wa_resp.raise_for_status()
        results["whatsapp"] = {
            "sent":     True,
            "from":     BUSINESS_NUMBER,
            "response": wa_resp.text.strip(),
        }
    except Exception as e:
        results["whatsapp"] = {"sent": False, "error": str(e)}

    # ── 2. SMS ────────────────────────────────────────────────
    try:
        sms_resp = requests.get(BHASH_SMS_URL, params={
            "user":     BHASH_USER,
            "password": BHASH_PASS,
            "mobile":   phone,
            "msg":      message,
            "pass":     BHASH_SENDER,
            "type":     "normal",
        }, timeout=10)
        sms_resp.raise_for_status()
        results["sms"] = {
            "sent":     True,
            "response": sms_resp.text.strip(),
        }
    except Exception as e:
        results["sms"] = {"sent": False, "error": str(e)}

    return {
        "status":  "processed",
        "to":      phone,
        "message": message,
        "results": results,
    }




const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
require('dotenv').config();

const app = express();
app.use(bodyParser.json());

const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;

// 🔹 Webhook verification (GET)
app.get('/webhook/facebook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  console.log({mode, token, challenge});

  if (mode && token === VERIFY_TOKEN) {
    console.log("Webhook verified");
    return res.status(200).send(challenge);
  } else {
    return res.sendStatus(403);
  }
});

app.get('/privacy', (req, res) => {
  res.status(200).send(`
    <h1>Privacy Policy</h1>
    <p>Last updated: ${new Date().toDateString()}</p>

    <p>Spectra Global Ltd. ("we", "our", "us") collects and processes user data obtained from Facebook Lead Ads.</p>

    <h2>Information We Collect</h2>
    <p>We may collect personal information such as name, email, phone number, and other details submitted through Facebook Lead Ads forms.</p>

    <h2>How We Use Information</h2>
    <p>The collected data is used solely for business communication, lead management, and customer support purposes.</p>

    <h2>Data Sharing</h2>
    <p>We do not sell or share your personal data with third parties.</p>

    <h2>Data Security</h2>
    <p>We take reasonable steps to protect your data from unauthorized access.</p>

    <h2>User Rights</h2>
    <p>You can request access or deletion of your data at any time.</p>

    <h2>Contact Us</h2>
    <p>Email: sgfxglobal@gmail.com</p>
  `);
});


app.get('/terms', (req, res) => {
  res.status(200).send(`
    <h1>Terms of Service</h1>
    <p>Last updated: ${new Date().toDateString()}</p>

    <p>By using our services, you agree to the following terms.</p>

    <h2>Use of Service</h2>
    <p>Our service is used to collect and manage leads from Facebook Lead Ads.</p>

    <h2>User Responsibility</h2>
    <p>You agree to provide accurate information when submitting forms.</p>

    <h2>Data Usage</h2>
    <p>Your submitted data may be used for communication and business purposes.</p>

    <h2>Limitation of Liability</h2>
    <p>We are not liable for any damages arising from the use of this service.</p>

    <h2>Contact</h2>
    <p>Email: sgfxglobal@gmail.com</p>
  `);
});

app.get('/delete-data', (req, res) => {
  res.send("Send request to sgfxglobal@gmail.com to delete your data.");
});

// 🔹 Receive webhook events (POST)
app.post('/webhook/facebook', async (req, res) => {
  console.log("Incoming webhook:", JSON.stringify(req.body, null, 2));

  try {
    const entry = req.body.entry?.[0];
    const change = entry?.changes?.[0];

    if (change?.field === 'leadgen') {
      const leadgenId = change.value.leadgen_id;
      console.log("Lead ID:", leadgenId);

      // 🔥 Fetch full lead data
      const url = `https://graph.facebook.com/v19.0/${leadgenId}?access_token=${PAGE_ACCESS_TOKEN}`;
      
      const response = await axios.get(url);

      console.log("Full Lead Data:", JSON.stringify(response.data, null, 2));
    }

    res.sendStatus(200);
  } catch (err) {
    console.error("Error:", err.response?.data || err.message);
    res.sendStatus(500);
  }
});

// 🔹 Health check
app.get('/', (req, res) => {
  res.send("Server running 🚀");
});

app.listen(process.env.PORT, () => {
  console.log(`Server running on port ${process.env.PORT}`);
});

const axios = require('axios');
require('dotenv').config();

//call : https://graph.facebook.com/v19.0/oauth/access_token
//   ?grant_type=fb_exchange_token
//   &client_id=YOUR_APP_ID
//   &client_secret=YOUR_APP_SECRET
//   &fb_exchange_token=SHORT_LIVED_TOKEN

const APP_ID = process.env.APP_ID;
const APP_SECRET = process.env.APP_SECRET;

console.log({APP_ID, APP_SECRET});

async function generateLongLivedToken() {
  try {
    const response = await axios.get('https://graph.facebook.com/v19.0/oauth/access_token', {
      params: {
        grant_type: 'fb_exchange_token',
        client_id: APP_ID,
        client_secret: APP_SECRET,
        fb_exchange_token: "EAAWjE243tIQBReb4Eyriduokh3yaY2921Mz7Y478mXEbF51kZARBP0KktUts8xyKNVex6wutyDkFmTwjo7sgVxWabEgbaWAK4t2xdv3HkNeCgsa9PEQnjgZCFATeyyc9wQxZCwW0DIxu8f1mZB5kXP1z6EJaSyeWxVrBm3ZAPaGpiVZCZA3cxiP7dkQ5rnnZAoDibZAuw9TPfR0DbiX7nqqei9yZA94tR9D7ZBHxU8wuT7yFK4w5dCZBuaKnbwn1igzTFUZAI653NEEX1ZCskt"
      }
    });
    console.log("Generated long-lived token:", response.data.access_token);
  } catch (error) {
    console.error("Error generating long-lived token:", error.response?.data || error.message);
  }
}

generateLongLivedToken();