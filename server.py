import os
import json
import asyncio
import traceback
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import deepl
from deepgram import DeepgramClient

# Environment variables
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DEEPL_API_KEY = os.getenv("DEEPL_API_KEY")
DEEPL_GLOSSARY_ID = os.getenv("DEEPL_GLOSSARY_ID")

app = FastAPI()
deepl_translator = deepl.DeepLClient(DEEPL_API_KEY) if DEEPL_API_KEY else None


def translate_text(text: str, source_lang: str) -> dict:
    if not deepl_translator:
        return {"source_lang": source_lang, "target_lang": "EN", "original_text": text, "translated_text": text}
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
        return {"source_lang": source_lang, "target_lang": "EN", "original_text": text, "translated_text": text}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    loop = asyncio.get_running_loop()

    if not DEEPGRAM_API_KEY:
        print("Error: DEEPGRAM_API_KEY is missing from environment variables.")
        await websocket.close(code=1008, reason="Missing API Key")
        return

    try:
        deepgram = DeepgramClient(DEEPGRAM_API_KEY)
        dg_connection = deepgram.listen.websocket.v("1")

        def on_message(self, result, **kwargs):
            try:
                if not result.channel or not result.channel.alternatives:
                    return
                
                sentence = result.channel.alternatives[0].transcript.strip()
                if sentence and result.is_final:
                    detected_lang = (
                        result.channel.alternatives[0].languages[0]
                        if getattr(result.channel.alternatives[0], "languages", None)
                        else "en"
                    )

                    async def process_and_send(text, lang):
                        payload = await loop.run_in_executor(None, translate_text, text, lang)
                        if payload:
                            await websocket.send_text(json.dumps(payload))

                    asyncio.run_coroutine_threadsafe(process_and_send(sentence, detected_lang), loop)
            except Exception as e:
                print(f"Error handling transcription: {e}\n{traceback.format_exc()}")

        dg_connection.on("Results", on_message)

        options = {
            "model": "nova-2",
            "language": "multi",
            "smart_format": True,
            "interim_results": False,
            "encoding": "linear16",
            "channels": 1,
            "sample_rate": 16000,
        }

        # Start Deepgram Connection inside executor to prevent blocking ASGI event loop
        started = await loop.run_in_executor(None, dg_connection.start, options)
        if not started:
            print("Failed to start Deepgram live connection.")
            await websocket.close(code=1011, reason="Deepgram Connection Failed")
            return

        try:
            while True:
                data = await websocket.receive_bytes()
                dg_connection.send(data)
        except WebSocketDisconnect:
            print("WebSocket disconnected cleanly.")
        except Exception as e:
            print(f"WebSocket send loop error: {e}")
        finally:
            await loop.run_in_executor(None, dg_connection.finish)

    except Exception as e:
        print(f"ASGI Application Exception in WebSocket: {e}")
        print(traceback.format_exc())
        await websocket.close(code=1011)


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
            button { width: 100%; padding: 16px; font-size: 18px; font-weight: bold; background: #28a745; color: white; border: none; border-radius: 12px; cursor: pointer; }
            #status { font-size: 13px; color: #888; text-align: center; margin-top: 6px; }
        </style>
    </head>
    <body>
        <button id="btn" onclick="toggle()">▶ START TRANSLATING SYSTEM AUDIO</button>
        <div id="status">Status: Idle</div>
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

            function setStatus(msg) {
                document.getElementById("status").innerText = "Status: " + msg;
            }

            async function toggle() {
                const btn = document.getElementById("btn");
                if (!active) {
                    try {
                        const protocol = location.protocol === "https:" ? "wss:" : "ws:";
                        socket = new WebSocket(`${protocol}//${location.host}/ws`);

                        socket.onopen = () => setStatus("Connected to Server. Streaming System Audio...");
                        socket.onclose = (e) => setStatus(`Disconnected (Code: ${e.code})`);
                        socket.onerror = (e) => setStatus("WebSocket Error");

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

                        stream = await navigator.mediaDevices.getDisplayMedia({
                            video: true,
                            audio: {
                                echoCancellation: false,
                                noiseSuppression: false,
                                autoGainControl: false
                            }
                        });

                        const audioTrack = stream.getAudioTracks()[0];
                        if (!audioTrack) {
                            alert("Important: You must check 'Share tab audio' or 'Share system audio' in the popup window!");
                            stream.getTracks().forEach(t => t.stop());
                            socket.close();
                            return;
                        }

                        audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
                        await audioCtx.resume();

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

                        btn.innerText = "⏹ STOP TRANSLATING";
                        btn.style.background = "#dc3545";
                        active = true;
                    } catch (err) {
                        alert("Audio Sharing Error: " + err.message);
                        setStatus("Failed to access system audio.");
                    }
                } else {
                    if (processor) processor.disconnect();
                    if (stream) stream.getTracks().forEach(t => t.stop());
                    if (socket) socket.close();
                    if (audioCtx) audioCtx.close();
                    btn.innerText = "▶ START TRANSLATING SYSTEM AUDIO";
                    btn.style.background = "#28a745";
                    setStatus("Stopped.");
                    active = false;
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
