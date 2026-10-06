import os
import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import deepl
from deepgram import DeepgramClient
from deepgram.clients.live.v1 import LiveOptions, LiveTranscriptionEvents

# Environment variables
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DEEPL_API_KEY = os.getenv("DEEPL_API_KEY")
DEEPL_GLOSSARY_ID = os.getenv("DEEPL_GLOSSARY_ID")

app = FastAPI()
deepl_translator = deepl.DeepLClient(DEEPL_API_KEY) if DEEPL_API_KEY else None


def translate_text(text: str, source_lang: str) -> dict:
    if not deepl_translator:
        return None
    try:
        target_lang = "EN-US" if source_lang.lower().startswith("ru") else "RU"
        kwargs = {"target_lang": target_lang}

        if target_lang == "RU" and DEEPL_GLOSSARY_ID:
            kwargs["glossary"] = DEEPL_GLOSSARY_ID

        result = deepl_translator.translate_text(text, **kwargs)
        return {
            "source_lang": source_lang,
            "target_lang": target_lang,
            "original_text": text,
            "translated_text": result.text,
        }
    except Exception as e:
        print(f"Translation Error: {e}")
        return None


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    loop = asyncio.get_running_loop()

    deepgram = DeepgramClient(DEEPGRAM_API_KEY)
    dg_connection = deepgram.listen.websocket.v("1")

    def on_message(self, result, **kwargs):
        sentence = result.channel.alternatives[0].transcript.strip()
        if sentence and result.is_final:
            detected_lang = (
                result.channel.alternatives[0].languages[0]
                if result.channel.alternatives[0].languages
                else "en"
            )

            translation_payload = loop.run_in_executor(
                None, translate_text, sentence, detected_lang
            )

            async def send_payload():
                payload = await translation_payload
                if payload:
                    await websocket.send_text(json.dumps(payload))

            asyncio.run_coroutine_threadsafe(send_payload(), loop)

    dg_connection.on(LiveTranscriptionEvents.Transcript, on_message)

    options = LiveOptions(
        model="nova-2",
        language="multi",
        smart_format=True,
        interim_results=False,
        encoding="linear16",
        channels=1,
        sample_rate=16000,
    )

    if not dg_connection.start(options):
        await websocket.close()
        return

    try:
        while True:
            data = await websocket.receive_bytes()
            dg_connection.send(data)
    except WebSocketDisconnect:
        pass
    finally:
        dg_connection.finish()


@app.get("/")
async def get_client():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Live Translator</title>
        <style>
            body { font-family: -apple-system, sans-serif; background: #111; color: #fff; margin: 0; padding: 12px; }
            .box { height: 38vh; border: 2px solid #333; border-radius: 12px; padding: 12px; background: #222; overflow-y: auto; margin-bottom: 12px; }
            h2 { margin-top: 0; font-size: 15px; color: #aaa; border-bottom: 1px solid #444; padding-bottom: 6px; }
            #en-box { border-color: #007acc; }
            #ru-box { border-color: #d9534f; }
            p { font-size: 17px; line-height: 1.4; margin: 0; }
            button { width: 100%; padding: 16px; font-size: 18px; font-weight: bold; background: #28a745; color: white; border: none; border-radius: 12px; }
        </style>
    </head>
    <body>
        <button id="btn" onclick="toggle()">▶ START TRANSLATING</button>
        <div style="margin-top:12px;">
            <div id="en-box" class="box">
                <h2>ENGLISH SUBTITLES (MAX 100 WORDS)</h2>
                <p id="en-text"></p>
            </div>
            <div id="ru-box" class="box">
                <h2>РУССКИЕ СУБТИТРЫ (MAX 100 WORDS)</h2>
                <p id="ru-text"></p>
            </div>
        </div>
        <script>
            let socket, audioCtx, stream, processor, active = false;

            function addWords(id, text) {
                const el = document.getElementById(id);
                let current = el.innerText.trim().split(/\\s+/).filter(Boolean);
                let incoming = text.trim().split(/\\s+/).filter(Boolean);
                let merged = [...current, ...incoming];
                
                if (merged.length > 100) merged = merged.slice(merged.length - 100);
                
                el.innerText = merged.join(" ");
                el.parentElement.scrollTop = el.parentElement.scrollHeight;
            }

            async function toggle() {
                const btn = document.getElementById("btn");
                if (!active) {
                    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
                    socket = new WebSocket(`${protocol}//${location.host}/ws`);

                    socket.onmessage = (e) => {
                        const data = JSON.parse(e.data);
                        if (data.target_lang.startsWith("EN")) {
                            addWords("en-text", data.translated_text);
                            addWords("ru-text", `[Orig]: ${data.original_text}`);
                        } else {
                            addWords("ru-text", data.translated_text);
                            addWords("en-text", `[Orig]: ${data.original_text}`);
                        }
                    };

                    stream = await navigator.mediaDevices.getUserMedia({ audio: { sampleRate: 16000, channelCount: 1 } });
                    audioCtx = new AudioContext({ sampleRate: 16000 });
                    const src = audioCtx.createMediaStreamSource(stream);
                    processor = audioCtx.createScriptProcessor(4096, 1, 1);
                    src.connect(processor);
                    processor.connect(audioCtx.destination);

                    processor.onaudioprocess = (e) => {
                        if (socket && socket.readyState === 1) {
                            const float32 = e.inputBuffer.getChannelData(0);
                            const int16 = new Int16Array(float32.length);
                            for (let i = 0; i < float32.length; i++) {
                                int16[i] = Math.max(-1, Math.min(1, float32[i])) * 0x7FFF;
                            }
                            socket.send(int16.buffer);
                        }
                    };

                    btn.innerText = "⏹ STOP";
                    btn.style.background = "#dc3545";
                    active = true;
                } else {
                    if (processor) processor.disconnect();
                    if (stream) stream.getTracks().forEach(t => t.stop());
                    if (socket) socket.close();
                    btn.innerText = "▶ START TRANSLATING";
                    btn.style.background = "#28a745";
                    active = false;
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
