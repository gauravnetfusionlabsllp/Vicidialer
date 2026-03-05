import os
import json
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from openai import OpenAI

# ─────────────────────────────────────────────
# CONFIG  — paste your OpenAI key below
# ─────────────────────────────────────────────
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

app = FastAPI(
    title="Call Recording Analyzer",
    description="Upload a call recording and get AI-powered agent performance rating (1-5 stars)",
    version="1.0.0"
)


# ─────────────────────────────────────────────
# REQUEST MODEL
# ─────────────────────────────────────────────
class TranscriptRequest(BaseModel):
    transcript: str


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
SUPPORTED_FORMATS = {".mp3", ".wav", ".m4a", ".ogg", ".webm", ".flac", ".mp4"}

SYSTEM_PROMPT = """You are an expert call center quality analyst with 15+ years of experience.
Evaluate the agent's performance on the call and respond ONLY with valid JSON — no markdown, no extra text.

Use this exact format:
{
  "overall_rating": <float 1.0-5.0>,
  "stars": <integer 1-5>,
  "summary": "<2-3 sentence overall summary>",
  "call_outcome": "<Successful Sale | Lead Generated | Not Converted | Support Resolved | Escalated>",
  "agent_sentiment": "<Positive | Neutral | Negative>",
  "client_sentiment": "<Positive | Neutral | Negative | Frustrated | Interested>",
  "categories": {
    "greeting_professionalism":  { "score": <1-5>, "comment": "<feedback>" },
    "product_knowledge":         { "score": <1-5>, "comment": "<feedback>" },
    "convincing_ability":        { "score": <1-5>, "comment": "<feedback>" },
    "objection_handling":        { "score": <1-5>, "comment": "<feedback>" },
    "communication_clarity":     { "score": <1-5>, "comment": "<feedback>" },
    "empathy_patience":          { "score": <1-5>, "comment": "<feedback>" },
    "closing_technique":         { "score": <1-5>, "comment": "<feedback>" }
  },
  "strengths":    ["<strength 1>", "<strength 2>", "<strength 3>"],
  "improvements": ["<area 1>",     "<area 2>",     "<area 3>"]
}

Rating scale:
1 = Very Poor   2 = Below Average   3 = Average   4 = Good   5 = Excellent
"""


# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────
def transcribe_audio(file_path: str) -> str:
    """Send audio file to OpenAI Whisper and return transcript text."""
    with open(file_path, "rb") as f:
        response = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            response_format="text"
        )

    return response


def rate_agent(transcript: str) -> dict:
    """Send transcript to GPT-4o and return structured performance rating."""
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.3,
        max_tokens=1500,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Analyze this call transcript and rate the agent's performance.\n"
                    "Focus especially on how well the agent convinced/persuaded the client "
                    "and handled objections.\n\n"
                    f"--- TRANSCRIPT ---\n{transcript}\n--- END ---"
                )
            }
        ]
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown fences if GPT wraps response in ```json ... ```
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1]
        if raw.startswith("json"):
            raw = raw[4:]

    return json.loads(raw.strip())


# ─────────────────────────────────────────────
# API ROUTES
# ─────────────────────────────────────────────

@app.get("/health")
def health():
    """Check if the server is running."""
    return {
        "status": "ok",
        "whisper_model": "whisper-1",
        "rating_model": "gpt-4o"
    }


@app.post("/analyze")
async def analyze_audio(audio: UploadFile = File(...)):
    """
    Upload an audio file and get agent performance rating.

    How to use:
    - Open http://localhost:8000/docs
    - Click POST /analyze -> Try it out -> Choose File -> Execute

    Supported formats: mp3, wav, m4a, ogg, webm, flac, mp4
    """

    # Check file format
    suffix = Path(audio.filename).suffix.lower()
    if suffix not in SUPPORTED_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported format '{suffix}'. Supported formats: {', '.join(SUPPORTED_FORMATS)}"
        )

    # Save uploaded file to a temporary location
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    try:
        content = await audio.read()
        tmp.write(content)
        tmp.flush()
        tmp.close()

        # Step 1 — Transcribe audio using Whisper
        print(f"[1/2] Transcribing: {audio.filename} ...")
        transcript = transcribe_audio(tmp.name)
        print(f"[1/2] Transcription done. Length: {len(transcript)} chars")

        # Step 2 — Rate agent performance using GPT-4o
        print(f"[2/2] Rating agent performance ...")
        result = rate_agent(transcript)
        print(f"[2/2] Rating done. Stars: {result.get('stars')}/5")

        # Add extra info to response
        result["transcript"] = transcript
        result["filename"]   = audio.filename

        return JSONResponse(content=result)

    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="GPT returned invalid JSON. Please try again.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Always clean up temp file
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)


@app.post("/analyze-text")
def analyze_text(body: TranscriptRequest):

    if not body.transcript.strip():
        raise HTTPException(status_code=400, detail="Transcript cannot be empty.")

    try:
        result = rate_agent(body.transcript)
        result["transcript"] = body.transcript
        return JSONResponse(content=result)

    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="GPT returned invalid JSON. Please try again.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


