import json
import logging
import uvicorn
# from fixa.bot import run_bot
# from fixa.scenario import Scenario
# from fixa.agent import Agent
from fastapi.responses import Response
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form, Request
from fastapi.middleware.cors import CORSMiddleware
# from twilio.rest import Client
import os

# from pydantic import BaseModel, Field
# import argparse
# from typing import Dict, Tuple, List, Literal, Optional
# from typing_extensions import TypedDict
# from openai.types.chat import ChatCompletionMessageParam

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# Global variables
# twilio_client: Optional[Client] = None
# port: Optional[int] = None
# ngrok_url: Optional[str] = None
'''
def set_args(server_port: int, server_ngrok_url: str):
    """Set the server arguments."""
    global port, ngrok_url
    port = server_port
    ngrok_url = server_ngrok_url

def set_twilio_client(client: Client):
    """Set the Twilio client."""
    global twilio_client
    twilio_client = client

# Store scenarios and agents by call_sid
active_pairs: Dict[str, Tuple[Scenario, Agent]] = {}

class CallStatus(TypedDict):
    status: Literal["in_progress", "completed", "error"]
    transcript: Optional[List[ChatCompletionMessageParam]]
    stereo_recording_url: Optional[str]
    error: Optional[str]

# Mapping from call_sid to status
call_status: Dict[str, CallStatus] = {}

app = FastAPI()
'''
import argparse
import requests
from pydantic import BaseModel, Field
from typing import Dict, Tuple, List, Literal, Optional
from typing_extensions import TypedDict

from twilio.rest import Client
from dotenv import load_dotenv
from openai.types.chat import ChatCompletionMessageParam

from fixa.bot import run_bot
from fixa.scenario import Scenario
from fixa.agent import Agent

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("unified_server")

# ---------------------------------------------------------------------------
# Global config and data
# ---------------------------------------------------------------------------

RECORD_CALLS = True  # If True, sets record=True on the outbound call
call_data: Dict[str, Dict] = {}  # Tracks call details by callSid

twilio_client: Optional[Client] = None
port: Optional[int] = None
ngrok_url: Optional[str] = None

app = FastAPI(
    title="Unified Twilio Pipeline Server",
    description="Serves TwiML + WebSocket real-time pipeline + test-runner endpoints",
    version="1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Datamodel & typed dict for statuses
# ---------------------------------------------------------------------------

class CallStatus(TypedDict):
    status: Literal["in_progress", "completed", "error", "unknown"]
    transcript: Optional[List[ChatCompletionMessageParam]]
    stereo_recording_url: Optional[str]
    error: Optional[str]


class OutboundCallRequest(BaseModel):
    """
    JSON body for /outbound requests.
    """
    to: str
    from_: str = Field(alias="from")
    scenario_prompt: str
    agent_prompt: str
    agent_voice_id: str = "79a125e8-cd45-4c13-8a67-188112f4dd22"


# ---------------------------------------------------------------------------
# Deepgram helper (if you want transcripts after the call completes)
# ---------------------------------------------------------------------------
'''
def fetch_transcript_from_deepgram(recording_url: str) -> List[str]:
    DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "")
    if not DEEPGRAM_API_KEY:
        logger.warning("DEEPGRAM_API_KEY not set, cannot fetch transcript from Deepgram")
        return []

    headers = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}
    endpoint = f"https://api.deepgram.com/v1/listen?url={recording_url}"
    resp = requests.get(endpoint, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        transcript = (
            data.get("results", {})
            .get("channels", [{}])[0]
            .get("alternatives", [{}])[0]
            .get("transcript", "")
        )
        logger.info(f"Fetched transcript from Deepgram: {transcript}")
        return [transcript] if transcript else []
    else:
        logger.warning(f"Deepgram API error: {resp.status_code} {resp.text}")
        return []
'''


def fetch_transcript_from_deepgram(recording_url: str) -> list:
    DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "")
    if not DEEPGRAM_API_KEY:
        print("Deepgram API key not set.")
        return []

    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
        "Content-Type": "application/json"
    }
    endpoint = "https://api.deepgram.com/v1/listen"
    payload = {"url": recording_url}

    response = requests.post(endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        transcript = data.get("results", {}).get("channels", [{}])[0].get("alternatives", [{}])[0].get("transcript", "")
        print(f"Fetched transcript from Deepgram: {transcript}")
        return [transcript] if transcript else []
    else:
        print(f"Deepgram API error: {response.status_code} {response.text}")
        return []


# ---------------------------------------------------------------------------
# TwiML route
# ---------------------------------------------------------------------------

def generate_stream_twiml() -> str:
    """
    Returns TwiML that <Connect><Stream> to /ws.
    No <Say> message, so it doesn't say "Connecting you..."
    """
    assert ngrok_url, "ngrok_url must be set"
    ws_url = ngrok_url.replace("https://", "")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://{ws_url}/ws" track="both_tracks" />
  </Connect>
</Response>
"""


@app.get("/twiml")
async def twiml_endpoint():
    """
    For inbound calls or for the Twilio calls.create(... url=...), we serve TwiML here
    that instructs Twilio to connect real-time audio to /ws.
    """
    twiml_response = generate_stream_twiml()
    return Response(content=twiml_response, media_type="application/xml")


# ---------------------------------------------------------------------------
# Outbound calls
# ---------------------------------------------------------------------------

@app.post("/outbound")
async def outbound_call(req: OutboundCallRequest):
    """
    Create an outbound call via Twilio, referencing /twiml for instructions.
    """
    global call_data, twilio_client, ngrok_url
    assert twilio_client, "Twilio client not initialized"
    assert ngrok_url, "ngrok_url not set"

    # Make the outbound call. The TwiML is fetched from /twiml
    logger.info("Placing an outbound call to %s", req.to)
    call = twilio_client.calls.create(
        record=RECORD_CALLS,
        recording_channels="dual",
        recording_status_callback=f"{ngrok_url}/recording",
        to=req.to,
        from_=req.from_,
        url=f"{ngrok_url}/twiml",
        method="GET",
        status_callback=f"{ngrok_url}/twilio_call_update",
        status_callback_method="POST",
        status_callback_event=["initiated", "ringing", "answered", "completed"],
    )
    call_sid = call.sid
    if not call_sid:
        raise ValueError("Twilio call SID is None. Outbound call failed.")

    # Store scenario + agent data so that /ws can pick it up
    scenario = Scenario(name="OutboundCall", prompt=req.scenario_prompt)
    agent = Agent(name="TenantCaller", prompt=req.agent_prompt, voice_id=req.agent_voice_id)
    call_data[call_sid] = {
        "status": "in_progress",
        "transcript": None,
        "stereo_recording_url": None,
        "error": None,
        "scenario": scenario,
        "agent": agent,
    }
    logger.info(f"Outbound call placed, callSid={call_sid}")

    return {"call_id": call_sid}


# ---------------------------------------------------------------------------
# Twilio status callback
# ---------------------------------------------------------------------------

@app.post("/twilio_call_update")
async def twilio_call_update(request: Request):
    """
    Twilio status callback: updates call_data with 'initiated', 'ringing', 'answered', 'completed'
    """
    global call_data
    form = await request.form()
    call_sid = form.get("CallSid")
    status = form.get("CallStatus", "unknown")
    recording_url = form.get("RecordingUrl")

    if not call_sid:
        logger.warning("No CallSid in twilio_call_update")
        return {"message": "No CallSid provided"}

    if call_sid not in call_data:
        logger.info(f"CallSid {call_sid} not found in call_data, creating new entry")
        call_data[call_sid] = {
            "status": "in_progress",
            "transcript": None,
            "stereo_recording_url": None,
            "error": None,
            "scenario": None,
            "agent": None,
        }

    call_data[call_sid]["status"] = status

    # If call completed, fetch transcript if we have a RecordingUrl
    if status == "completed" and recording_url:
        rec_url = recording_url + ".mp3"
        call_data[call_sid]["stereo_recording_url"] = rec_url
        if not call_data[call_sid]["transcript"]:
            # Optionally fetch from Deepgram
            call_data[call_sid]["transcript"] = fetch_transcript_from_deepgram(rec_url)

    logger.info(f"[twilio_call_update] callSid={call_sid}, status={status}, recordingUrl={recording_url}")
    return {"message": "OK"}


# ---------------------------------------------------------------------------
# Recording callback
# ---------------------------------------------------------------------------

@app.post("/recording")
async def recording_callback(RecordingSid: str = Form(), RecordingUrl: str = Form(), CallSid: str = Form()):
    """
    Twilio calls this if 'recording_status_callback' is set on the call creation
    """
    global call_data
    logger.info(f"Recording SID={RecordingSid}, RecordingUrl={RecordingUrl}, CallSid={CallSid}")
    if CallSid in call_data:
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        rec_url = RecordingUrl + ".mp3" if RecordingUrl else ""
        if account_sid and auth_token and rec_url:
            # If we want to store an authenticated URL
            base_url = rec_url.replace("https://", "")
            authenticated_url = f"https://{account_sid}:{auth_token}@{base_url}"
            call_data[CallSid]["stereo_recording_url"] = authenticated_url
        # If the recording arrives before the call is 'completed',
        # you might mark it "error" or just let it ride.
        # For now, do nothing special.
    return {"success": True}


# ---------------------------------------------------------------------------
# Route for test runner to poll call statuses
# ---------------------------------------------------------------------------

@app.get("/call_status")
async def call_status():
    """
    Called by the TestRunner to see if calls are completed, etc.
    """
    return call_data


# ---------------------------------------------------------------------------
# Real-time /ws pipeline route
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Twilio hits /ws in real time after reading the TwiML <Connect><Stream>.
    We parse the first "start" frame to get callSid & streamSid,
    then pass scenario/agent to run_bot(...).
    """
    await websocket.accept()
    try:
        first_frame = await websocket.receive_text()
        frame_data = json.loads(first_frame)
        call_sid = frame_data["start"]["callSid"]
        stream_sid = frame_data["start"]["streamSid"]
        logger.info(f"WebSocket connected for callSid={call_sid}, streamSid={stream_sid}")

        if call_sid not in call_data:
            logger.error(f"callSid={call_sid} not found in call_data, cannot run pipeline.")
            return

        scenario = call_data[call_sid]["scenario"]
        agent = call_data[call_sid]["agent"]
        if not scenario or not agent:
            logger.error(f"No scenario or agent stored for callSid={call_sid}")
            return

        # Now run the real-time pipeline
        transcript = await run_bot(agent, scenario, websocket, stream_sid, call_sid)
        # Once run_bot finishes, we update call_data if not yet "error"
        if call_data[call_sid]["status"] != "error":
            call_data[call_sid]["status"] = "completed"
            call_data[call_sid]["transcript"] = transcript
        logger.info(f"Pipeline finished for callSid={call_sid}")
    except WebSocketDisconnect:
        logger.warning("Twilio WebSocket disconnected abruptly")
    except Exception as e:
        logger.exception(f"Error in /ws pipeline: {e}")
        if call_sid in call_data:
            call_data[call_sid]["status"] = "error"
            call_data[call_sid]["error"] = str(e)
    finally:
        # Clean up or keep call_data if you want transcripts/evals
        pass


# ---------------------------------------------------------------------------
# Startup: parse arguments, set Twilio, run uvicorn
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ngrok_url", type=str, required=True)
    args = parser.parse_args()

    port = args.port
    ngrok_url = args.ngrok_url

    TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
    TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
    twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

    logger.info(f"Starting unified server on port={port}, domain={ngrok_url}")
    uvicorn.run(app, host="0.0.0.0", port=port)


def get_stream_twiml() -> str:
    """
    Returns the TwiML for the websocket stream.
    """
    assert ngrok_url is not None
    ws_url = ngrok_url.replace('https://', '')
    return f"<Response><Connect><Stream url='wss://{ws_url}/ws'></Stream></Connect></Response>"


class OutboundCallRequest(BaseModel):
    to: str
    from_: str = Field(alias='from')
    scenario_prompt: str
    agent_prompt: str
    agent_voice_id: str = "79a125e8-cd45-4c13-8a67-188112f4dd22"  # Default to British Lady


class TranscriptRequest(BaseModel):
    call_sid: str
    transcript: List[ChatCompletionMessageParam]


class RecordingRequest(BaseModel):
    RecordingUrl: str
    CallSid: str


@app.get("/status")
async def status():
    return call_status


@app.post("/outbound")
async def outbound_call(request: OutboundCallRequest):
    assert twilio_client is not None, "Twilio client not initialized"
    assert ngrok_url is not None, "ngrok URL not set"

    call = twilio_client.calls.create(
        record=True,
        recording_channels="dual",
        recording_status_callback=f"{ngrok_url}/recording",
        to=request.to,
        from_=request.from_,
        twiml=get_stream_twiml(),
    )
    call_sid = call.sid
    if call_sid is None:
        raise ValueError("Call SID is None")

    # Create scenario and agent
    scenario = Scenario(name="outbound_call", prompt=request.scenario_prompt)
    agent = Agent(name="agent", prompt=request.agent_prompt, voice_id=request.agent_voice_id)

    # Store them for this call
    active_pairs[call_sid] = (scenario, agent)

    # Set the status to in_progress
    call_status[call_sid] = {
        "status": "in_progress",
        "transcript": None,
        "stereo_recording_url": None,
        "error": None
    }
    logger.info(f"OUTBOUND CALL {call_sid} to {request.to}")
    return {"success": True, "call_id": call_sid}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    first_frame = await websocket.receive_text()
    call_data = json.loads(first_frame)
    stream_sid = call_data["start"]["streamSid"]
    call_sid = call_data["start"]["callSid"]
    logger.info(f"WebSocket connection accepted for call {call_sid}")

    # Get the scenario and agent for this call
    pair = active_pairs.get(call_sid)
    if not pair:
        logger.error(f"No scenario/agent pair found for call {call_sid}")
        return

    scenario, agent = pair
    try:
        transcript = await run_bot(agent, scenario, websocket, stream_sid, call_sid)
        call_status[call_sid] = {
            "status": "completed",
            "transcript": transcript,
            "stereo_recording_url": None,
            "error": None
        }
    except Exception as e:
        logger.error(f"Bot failed for call {call_sid}: {str(e)}")
        call_status[call_sid] = {
            "status": "error",
            "transcript": None,
            "stereo_recording_url": None,
            "error": str(e)
        }
    finally:
        del active_pairs[call_sid]


@app.post("/recording")
async def recording(RecordingSid: str = Form(), RecordingUrl: str = Form(), CallSid: str = Form()):
    logger.info(f"Recording SID: {RecordingSid}, Recording URL: {RecordingUrl}, Call SID: {CallSid}")
    if CallSid in call_status:
        # Format the recording URL with authentication credentials
        account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        base_url = RecordingUrl.replace("https://", "")
        authenticated_url = f"https://{account_sid}:{auth_token}@{base_url}"
        call_status[CallSid]["stereo_recording_url"] = authenticated_url
        if call_status[CallSid]["status"] != "completed":
            # If recording is received before call is completed, mark as error
            call_status[CallSid]["status"] = "error"
            call_status[CallSid]["error"] = "agent failed to start"
    return {"success": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ngrok_url", type=str, required=True)
    parsed_args = parser.parse_args()

    set_args(parsed_args.port, parsed_args.ngrok_url)
    set_twilio_client(Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN")))

    assert port is not None, "Port not set"
    uvicorn.run(app, host="0.0.0.0", port=port)

# python server.py --port 8765
