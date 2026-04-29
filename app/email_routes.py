# ═══════════════════════════════════════════════════════════════
#  EMAIL TEMPLATES & ATTACHMENTS  —  Admin Only
#  Drop this file next to main.py and add to your app:
#
#    from email_routes import email_router
#    app.include_router(email_router)
#
#  PostgreSQL table setup runs automatically on first import.
# ═══════════════════════════════════════════════════════════════

import os
import uuid
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dependencies import get_current_user, get_pg_conn
# ── Re-use your existing helpers (imported from Correct_demo_code) ─────────
# These are already defined in your Correct_demo_code.py; import them here.
# If you paste this code directly into Correct_demo_code.py, remove these imports.
from Correct_demo_code import get_current_user, get_pg_conn

import psycopg2
import psycopg2.extras

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ATTACHMENT_DIR = Path("email_attachments")
ATTACHMENT_DIR.mkdir(exist_ok=True)

ALLOWED_MIME_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "text/plain", "text/csv",
    "application/zip",
}
MAX_FILE_SIZE_MB = 10

email_router = APIRouter(prefix="/email", tags=["Email Templates"])


# ─────────────────────────────────────────────
# POSTGRES TABLE SETUP  (runs once on import)
# ─────────────────────────────────────────────
def init_email_tables():
    conn = get_pg_conn()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_templates (
            id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name         TEXT NOT NULL,
            subject      TEXT NOT NULL,
            body         TEXT NOT NULL,
            created_by   TEXT NOT NULL,
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS email_attachments (
            id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            original_name TEXT NOT NULL,
            stored_name   TEXT NOT NULL,
            file_path     TEXT NOT NULL,
            file_type     TEXT NOT NULL,
            file_size     BIGINT NOT NULL,
            description   TEXT,
            uploaded_by   TEXT NOT NULL,
            uploaded_at   TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS email_template_attachments (
            template_id   UUID NOT NULL REFERENCES email_templates(id)   ON DELETE CASCADE,
            attachment_id UUID NOT NULL REFERENCES email_attachments(id) ON DELETE CASCADE,
            PRIMARY KEY (template_id, attachment_id)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

init_email_tables()


# ─────────────────────────────────────────────
# GUARD: admin only
# ─────────────────────────────────────────────
def require_admin(current_user: dict = Depends(get_current_user)):
    if not current_user.get("isAdmin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# ─────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────
class TemplateCreate(BaseModel):
    name:           str
    subject:        str
    body:           str
    attachment_ids: Optional[List[str]] = []

class TemplateUpdate(BaseModel):
    name:           Optional[str]       = None
    subject:        Optional[str]       = None
    body:           Optional[str]       = None
    attachment_ids: Optional[List[str]] = None   # None = don't touch, [] = clear all


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _attach_url(stored_name: str) -> str:
    return f"/email_attachments/{stored_name}"

def _get_template_with_attachments(cur, template_id: str) -> dict:
    cur.execute(
        "SELECT * FROM email_templates WHERE id = %s",
        (template_id,)
    )
    row = cur.fetchone()
    if not row:
        return None
    t = dict(row)
    cur.execute("""
        SELECT ea.*
        FROM email_attachments ea
        JOIN email_template_attachments eta ON ea.id = eta.attachment_id
        WHERE eta.template_id = %s
        ORDER BY ea.uploaded_at DESC
    """, (template_id,))
    t["attachments"] = [
        {**dict(a), "url": _attach_url(a["stored_name"])}
        for a in cur.fetchall()
    ]
    return t

def _link_attachments(cur, template_id: str, attachment_ids: List[str]):
    """Replace all attachment links for a template."""
    cur.execute(
        "DELETE FROM email_template_attachments WHERE template_id = %s",
        (template_id,)
    )
    for att_id in attachment_ids:
        cur.execute(
            "SELECT id FROM email_attachments WHERE id = %s", (att_id,)
        )
        if cur.fetchone():
            cur.execute(
                "INSERT INTO email_template_attachments VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (template_id, att_id)
            )


# ═══════════════════════════════════════════════════════════════
#  TEMPLATE ROUTES
# ═══════════════════════════════════════════════════════════════

@email_router.get("/templates")
def list_templates(admin=Depends(require_admin)):
    """List all email templates with their linked attachments."""
    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id FROM email_templates ORDER BY created_at DESC")
        ids    = [r["id"] for r in cur.fetchall()]
        result = [_get_template_with_attachments(cur, str(tid)) for tid in ids]
        return {"count": len(result), "data": result}
    finally:
        cur.close(); conn.close()


@email_router.get("/templates/{template_id}")
def get_template(template_id: str, admin=Depends(require_admin)):
    """Get a single email template."""
    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        t = _get_template_with_attachments(cur, template_id)
        if not t:
            raise HTTPException(404, "Template not found")
        return t
    finally:
        cur.close(); conn.close()


@email_router.post("/templates", status_code=201)
def create_template(payload: TemplateCreate, admin=Depends(require_admin)):
    """
    Create a new email template.
    Body supports placeholders like {{client_name}}, {{amount}}, {{agent_name}}.
    Optionally link existing attachment IDs.
    """
    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        template_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO email_templates (id, name, subject, body, created_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (template_id, payload.name, payload.subject,
              payload.body, admin["username"]))

        if payload.attachment_ids:
            _link_attachments(cur, template_id, payload.attachment_ids)

        conn.commit()
        t = _get_template_with_attachments(cur, template_id)
        return {"message": "Template created", "data": t}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()


@email_router.put("/templates/{template_id}")
def update_template(
    template_id: str,
    payload:     TemplateUpdate,
    admin=Depends(require_admin)
):
    """Update template fields and/or replace its attachment list."""
    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id FROM email_templates WHERE id = %s", (template_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Template not found")

        fields, values = [], []
        if payload.name    is not None: fields.append("name = %s");    values.append(payload.name)
        if payload.subject is not None: fields.append("subject = %s"); values.append(payload.subject)
        if payload.body    is not None: fields.append("body = %s");    values.append(payload.body)

        if fields:
            fields.append("updated_at = NOW()")
            values.append(template_id)
            cur.execute(
                f"UPDATE email_templates SET {', '.join(fields)} WHERE id = %s",
                values
            )

        if payload.attachment_ids is not None:
            _link_attachments(cur, template_id, payload.attachment_ids)

        conn.commit()
        t = _get_template_with_attachments(cur, template_id)
        return {"message": "Template updated", "data": t}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()


@email_router.delete("/templates/{template_id}")
def delete_template(template_id: str, admin=Depends(require_admin)):
    """Delete a template (attachments stay, only the link is removed)."""
    conn = get_pg_conn()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT id FROM email_templates WHERE id = %s", (template_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Template not found")
        cur.execute("DELETE FROM email_templates WHERE id = %s", (template_id,))
        conn.commit()
        return {"message": "Template deleted"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()


# ═══════════════════════════════════════════════════════════════
#  ATTACHMENT ROUTES
# ═══════════════════════════════════════════════════════════════

@email_router.get("/attachments")
def list_attachments(admin=Depends(require_admin)):
    """List all uploaded attachments."""
    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM email_attachments ORDER BY uploaded_at DESC")
        rows = cur.fetchall()
        return {
            "count": len(rows),
            "data":  [{**dict(r), "url": _attach_url(r["stored_name"])} for r in rows]
        }
    finally:
        cur.close(); conn.close()


@email_router.post("/attachments", status_code=201)
async def upload_attachment(
    file:        UploadFile      = File(...),
    description: Optional[str]   = Form(None),
    admin=Depends(require_admin)
):
    """
    Upload an attachment.
    Supports: images (jpg/png/gif/webp), PDF, Word, Excel, TXT, CSV, ZIP.
    Max size: 10 MB.
    """
    if file.content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            400,
            f"File type '{file.content_type}' not allowed. "
            "Accepted: images, PDF, Word, Excel, TXT, CSV, ZIP."
        )

    content = await file.read()
    if len(content) > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(400, f"File exceeds {MAX_FILE_SIZE_MB} MB limit.")

    ext         = Path(file.filename).suffix.lower()
    stored_name = f"{uuid.uuid4()}{ext}"
    file_path   = ATTACHMENT_DIR / stored_name

    with open(file_path, "wb") as f:
        f.write(content)

    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        att_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO email_attachments
                (id, original_name, stored_name, file_path, file_type, file_size, description, uploaded_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            att_id, file.filename, stored_name, str(file_path),
            file.content_type, len(content), description, admin["username"]
        ))
        conn.commit()
        return {
            "message":       "Attachment uploaded",
            "id":            att_id,
            "original_name": file.filename,
            "url":           _attach_url(stored_name),
            "file_type":     file.content_type,
            "file_size":     len(content),
        }
    except Exception as e:
        conn.rollback()
        file_path.unlink(missing_ok=True)
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()


@email_router.delete("/attachments/{attachment_id}")
def delete_attachment(attachment_id: str, admin=Depends(require_admin)):
    """Delete an attachment from disk and database."""
    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM email_attachments WHERE id = %s", (attachment_id,))
        att = cur.fetchone()
        if not att:
            raise HTTPException(404, "Attachment not found")

        # Remove from disk
        try:
            Path(att["file_path"]).unlink(missing_ok=True)
        except Exception:
            pass

        cur.execute("DELETE FROM email_attachments WHERE id = %s", (attachment_id,))
        conn.commit()
        return {"message": f"Attachment '{att['original_name']}' deleted"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()


# ═══════════════════════════════════════════════════════════════
#  AGENT READ-ONLY ROUTE
#  Agent calls this when the mail button is clicked.
#  Returns all templates + pre-signed attachment URLs ready to use.
# ═══════════════════════════════════════════════════════════════

@email_router.get("/agent/templates", tags=["Agent Email"])
def agent_get_templates(current_user: dict = Depends(get_current_user)):
    """
    Read-only endpoint for agents.
    Returns all active email templates with their attachment URLs.
    Used when agent clicks the mail button to pick a template.
    """
    conn = get_pg_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id FROM email_templates ORDER BY name ASC")
        ids    = [r["id"] for r in cur.fetchall()]
        result = [_get_template_with_attachments(cur, str(tid)) for tid in ids]
        return {
            "count": len(result),
            "agent": current_user["username"],
            "data":  result
        }
    finally:
        cur.close(); conn.close()
