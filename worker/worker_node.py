import argparse
import os
import sys
import threading
import time
import uuid
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template_string, request, send_file
import requests

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common.config import ensure_firewall_allowed, get_local_ip
from worker.transcoder import transcode_video_chunk

app = Flask(__name__)

PROCESSED_REQUESTS: Dict[str, Dict[str, Any]] = {}
WORKER_TASKS: List[Dict[str, Any]] = []
LATEST_CHUNK_INFO: Dict[str, Any] = {}
MASTER_NODE_URL = ""

INSTANCE_ID = str(uuid.uuid4())[:8]
WORKER_ID = "Worker-1"
WORKER_PORT = 5001


HTML_WORKER_DASHBOARD = """<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DistriStream — Worker Node ({{ worker_id }})</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;700;800;900&family=JetBrains+Mono:wght@600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-canvas: #FAF8F0;
            --bg-grid: #E8E5D8;
            --surface: #FFFFFF;
            --border: #000000;
            --border-width: 3px;
            --shadow-hard: 4px 4px 0px #000000;
            --shadow-sm: 2px 2px 0px #000000;
            --yellow-main: #FFE600;
            --green-neo: #00E599;
            --blue-neo: #38BDF8;
            --coral-neo: #FF5A5F;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; color: #000; }
        body {
            background-color: var(--bg-canvas);
            background-image: radial-gradient(var(--bg-grid) 2px, transparent 2px);
            background-size: 24px 24px;
            padding: 1.5rem;
        }
        .container { max-width: 900px; margin: 0 auto; }
        .header {
            background: var(--yellow-main);
            border: var(--border-width) solid var(--border);
            border-radius: 16px;
            box-shadow: var(--shadow-hard);
            padding: 1.25rem 1.75rem;
            margin-bottom: 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }
        .title { font-size: 1.5rem; font-weight: 900; }
        .badge-online {
            background: var(--green-neo);
            border: 2px solid var(--border);
            border-radius: 999px;
            padding: 0.3rem 0.8rem;
            font-weight: 900;
            font-size: 0.85rem;
            box-shadow: var(--shadow-sm);
        }
        .card {
            background: var(--surface);
            border: var(--border-width) solid var(--border);
            border-radius: 16px;
            box-shadow: var(--shadow-hard);
            padding: 1.5rem;
            margin-bottom: 1.5rem;
        }
        .card-title {
            font-size: 1.15rem;
            font-weight: 900;
            margin-bottom: 1rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        .info-box {
            background: #FFFDF8;
            border: 2px solid var(--border);
            border-radius: 10px;
            padding: 0.75rem 1rem;
            box-shadow: var(--shadow-sm);
        }
        .info-label { font-size: 0.75rem; font-weight: 800; color: #666; text-transform: uppercase; }
        .info-val { font-size: 1rem; font-weight: 900; margin-top: 0.2rem; }
        .video-container {
            background: #000;
            border: 3px solid #000;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: var(--shadow-hard);
            max-width: 100%;
        }
        video { width: 100%; display: block; max-height: 400px; background: #000; }
        table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; font-size: 0.85rem; }
        th, td { border: 2px solid #000; padding: 0.5rem 0.75rem; text-align: left; }
        th { background: var(--yellow-main); font-weight: 900; }
        tr:nth-child(even) { background: #F9F9F9; }
        .btn-link {
            display: inline-block;
            background: var(--blue-neo);
            border: 2px solid #000;
            border-radius: 8px;
            padding: 0.4rem 0.8rem;
            font-weight: 800;
            text-decoration: none;
            box-shadow: var(--shadow-sm);
            font-size: 0.82rem;
            margin-top: 0.5rem;
        }
        .btn-link:hover { transform: translate(-2px, -2px); box-shadow: 4px 4px 0px #000; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <div class="title">⚡ DistriStream Worker Node</div>
                <div style="font-size: 0.85rem; font-weight: 700; color: #444;">Node Eksekusi Transcoding Terdistribusi</div>
            </div>
            <div style="display: flex; gap: 0.5rem; align-items: center;">
                <span style="font-weight: 900; font-size: 1rem;">{{ worker_id }}</span>
                <span class="badge-online">ONLINE</span>
            </div>
        </div>

        <div class="card">
            <div class="card-title">📌 Informasi Node & Koneksi Master</div>
            <div class="info-grid">
                <div class="info-box">
                    <div class="info-label">Symbolic ID</div>
                    <div class="info-val" id="dispWorkerId">{{ worker_id }}</div>
                </div>
                <div class="info-box">
                    <div class="info-label">Port Lokal</div>
                    <div class="info-val">:{{ port }}</div>
                </div>
                <div class="info-box" style="grid-column: span 2;">
                    <div class="info-label">Tersambung ke Master</div>
                    <div class="info-val" id="dispMaster">
                        {% if master_url %}
                            <a href="{{ master_url }}" target="_blank" style="color: #2563EB; text-decoration: underline;">{{ master_url }}</a>
                        {% else %}
                            <span style="color: #666;">Mode Lokal (Standalone / run_cluster)</span>
                        {% endif %}
                    </div>
                </div>
            </div>
        </div>

        <div class="card" id="chunkPreviewCard">
            <div class="card-title">🎬 Live Preview Chunk Transcode di Laptop Ini</div>
            <div id="noChunkNotice" style="padding: 1.5rem; text-align: center; border: 2px dashed #000; border-radius: 12px; background: #FFFDF8;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">⏳</div>
                <div style="font-weight: 800; font-size: 1rem;">Menunggu Scatter Chunk dari Master...</div>
                <div style="font-size: 0.82rem; color: #666; margin-top: 0.25rem;">Ketika Master membagi video, potongan yang ditugaskan ke node ini akan otomatis diputar di sini secara real-time.</div>
            </div>
            <div id="chunkPlayerBox" style="display: none;">
                <div class="video-container">
                    <video id="workerVideo" controls autoplay loop playsinline></video>
                </div>
                <div style="margin-top: 0.75rem; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 0.5rem;">
                    <div>
                        <span id="chunkTag" style="background: var(--green-neo); border: 2px solid #000; border-radius: 6px; padding: 0.2rem 0.6rem; font-weight: 900; font-size: 0.8rem; box-shadow: var(--shadow-sm);">CHUNK</span>
                        <span id="chunkName" style="font-weight: 800; margin-left: 0.4rem; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem;"></span>
                    </div>
                    <div id="chunkMeta" style="font-size: 0.8rem; font-weight: 800; color: #555;"></div>
                </div>
            </div>
        </div>

        <div class="card">
            <div class="card-title">📋 Riwayat Chunk yang Selesai di Node Ini</div>
            <div style="overflow-x: auto;">
                <table id="tasksTable">
                    <thead>
                        <tr>
                            <th>Waktu</th>
                            <th>Chunk Masuk</th>
                            <th>Hasil Transcode</th>
                            <th>Resolusi</th>
                            <th>Durasi</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody id="tasksBody">
                        <tr>
                            <td colspan="6" style="text-align: center; color: #777;">Belum ada tugas transcoding yang selesai.</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        let currentChunkFile = null;

        async function pollWorkerStatus() {
            try {
                const response = await fetch('/api/v1/worker/status');
                const data = await response.json();

                if (data.latest_chunk && data.latest_chunk.output_filename) {
                    document.getElementById('noChunkNotice').style.display = 'none';
                    document.getElementById('chunkPlayerBox').style.display = 'block';

                    const filename = data.latest_chunk.output_filename;
                    document.getElementById('chunkName').innerText = filename;
                    document.getElementById('chunkMeta').innerText = `${data.latest_chunk.resolution} | Durasi: ${(data.latest_chunk.duration_ms / 1000).toFixed(2)}s | Selesai: ${data.latest_chunk.time}`;

                    if (currentChunkFile !== filename) {
                        currentChunkFile = filename;
                        const v = document.getElementById('workerVideo');
                        v.src = '/stream/chunk?t=' + Date.now();
                        v.play().catch(() => {});
                    }
                }

                if (data.tasks && data.tasks.length > 0) {
                    const tbody = document.getElementById('tasksBody');
                    tbody.innerHTML = data.tasks.map(t => `
                        <tr>
                            <td style="font-family: 'JetBrains Mono', monospace;">${t.time}</td>
                            <td style="font-weight: 700;">${t.chunk_filename || '-'}</td>
                            <td style="font-family: 'JetBrains Mono', monospace; font-weight: 800; color: #2563EB;">${t.output_filename || '-'}</td>
                            <td><span style="background: var(--yellow-main); border: 1.5px solid #000; border-radius: 4px; padding: 0.1rem 0.35rem; font-weight: 800; font-size: 0.75rem;">${t.resolution}</span></td>
                            <td style="font-weight: 800;">${t.duration_ms} ms</td>
                            <td><span style="background: ${t.status === 'SUCCESS' ? 'var(--green-neo)' : 'var(--coral-neo)'}; border: 1.5px solid #000; border-radius: 4px; padding: 0.1rem 0.4rem; font-weight: 900; font-size: 0.75rem;">${t.status}</span></td>
                        </tr>
                    `).join('');
                }
            } catch (err) {
                console.error(err);
            }
        }

        setInterval(pollWorkerStatus, 2000);
        pollWorkerStatus();
    </script>
</body>
</html>"""


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ONLINE",
        "worker_id": WORKER_ID,
        "instance_id": INSTANCE_ID,
        "port": WORKER_PORT,
        "attributes": {
            "codec": "H.264/AAC",
            "gpu_accel": False,
            "max_concurrent": 2,
        },
    }), 200


@app.route("/rpc/transcode", methods=["POST"])
def handle_transcode_rpc():
    payload = request.get_json()
    if not payload:
        return jsonify({"success": False, "error": "Invalid request payload"}), 400

    task_id = payload.get("task_id")
    sequence_number = payload.get("seq_num", 1)
    input_path = payload.get("input_path")
    output_path = payload.get("output_path")
    resolution = payload.get("target_resolution", "720p")

    video_id = payload.get("video_id")
    chunk_filename = payload.get("chunk_filename")
    output_filename = payload.get("output_filename")
    master_url = payload.get("master_url")

    cache_key = f"{task_id}_{sequence_number}"
    if cache_key in PROCESSED_REQUESTS:
        print(f"[{WORKER_ID}] Cache hit for duplicate request: {cache_key}")
        return jsonify(PROCESSED_REQUESTS[cache_key]), 200

    print(f"\n[{WORKER_ID}] Executing RPC Task: {task_id}")
    print(f"[{WORKER_ID}]   Target Resolution: {resolution}")

    # Deteksi apakah berkas input ada secara lokal atau perlu diunduh dari Master (skenario beda laptop)
    is_remote_flag = payload.get("is_remote")
    if is_remote_flag is not None:
        is_remote_mode = bool(is_remote_flag and master_url and chunk_filename and video_id)
    else:
        is_remote_mode = (not os.path.exists(input_path)) and bool(master_url and chunk_filename and video_id)

    actual_input_path = input_path
    actual_output_path = output_path

    if is_remote_mode:
        local_chunk_dir = os.path.join(ROOT_DIR, "storage", "chunks", video_id)
        os.makedirs(local_chunk_dir, exist_ok=True)
        actual_input_path = os.path.join(local_chunk_dir, chunk_filename)
        actual_output_path = os.path.join(local_chunk_dir, output_filename or f"{chunk_filename}_{resolution}.mp4")

        download_endpoint = f"{master_url.rstrip('/')}/api/v1/chunks/{video_id}/{chunk_filename}"
        print(f"[{WORKER_ID}] 📥 Mengunduh chunk dari Master: {download_endpoint}...")
        try:
            download_response = requests.get(download_endpoint, timeout=120)
            if download_response.status_code != 200:
                err_msg = f"Gagal mengunduh chunk dari Master: HTTP {download_response.status_code}"
                print(f"[{WORKER_ID}] ❌ {err_msg}")
                return jsonify({"success": False, "error": err_msg}), 500
            with open(actual_input_path, "wb") as chunk_file:
                chunk_file.write(download_response.content)
            size_kb = len(download_response.content) / 1024
            print(f"[{WORKER_ID}] ✅ Chunk berhasil diunduh ({size_kb:.1f} KB)")
        except Exception as error:
            err_msg = f"Koneksi unduh chunk ke Master ({master_url}) gagal: {error}"
            print(f"[{WORKER_ID}] ❌ {err_msg}")
            return jsonify({"success": False, "error": err_msg}), 500

    # Eksekusi transcoding lokal menggunakan CPU/GPU laptop ini
    print(f"[{WORKER_ID}] ⚙️ Memulai transcode {chunk_filename or os.path.basename(actual_input_path)} -> {resolution}...")
    try:
        result = transcode_video_chunk(actual_input_path, actual_output_path, target_resolution=resolution)
    except Exception as error:
        err_msg = f"Transcode gagal: {error}"
        print(f"[{WORKER_ID}] ❌ {err_msg}")
        return jsonify({"success": False, "error": err_msg}), 500

    result["worker_id"] = WORKER_ID
    result["task_id"] = task_id
    result["seq_num"] = sequence_number

    # Jika mode multi-laptop, kirim kembali chunk yang sudah selesai ke Master
    if is_remote_mode and result.get("success"):
        upload_endpoint = f"{master_url.rstrip('/')}/api/v1/chunks/{video_id}/{output_filename}"
        file_size_kb = os.path.getsize(actual_output_path) / 1024 if os.path.exists(actual_output_path) else 0
        print(f"[{WORKER_ID}] 📤 Mengirim balik hasil transcode ke Master ({file_size_kb:.1f} KB): {output_filename}...")
        try:
            with open(actual_output_path, "rb") as transcoded_file:
                files_payload = {"chunk_file": (output_filename, transcoded_file, "video/mp4")}
                upload_response = requests.post(upload_endpoint, files=files_payload, timeout=120)
                if upload_response.status_code != 200:
                    err_msg = f"Gagal mengunggah chunk hasil ke Master (HTTP {upload_response.status_code}): {upload_response.text}"
                    print(f"[{WORKER_ID}] ❌ {err_msg}")
                    return jsonify({"success": False, "error": err_msg}), 500
                print(f"[{WORKER_ID}] ✅ Berhasil mengunggah hasil transcode ke Master")
        except Exception as error:
            err_msg = f"Koneksi kirim balik ke Master ({master_url}) gagal: {error}"
            print(f"[{WORKER_ID}] ❌ {err_msg}")
            return jsonify({"success": False, "error": err_msg}), 500

    PROCESSED_REQUESTS[cache_key] = result

    global LATEST_CHUNK_INFO
    if result.get("success") and os.path.exists(actual_output_path):
        LATEST_CHUNK_INFO = {
            "video_id": video_id,
            "chunk_filename": chunk_filename,
            "output_filename": output_filename or os.path.basename(actual_output_path),
            "resolution": resolution,
            "duration_ms": result.get("duration_ms", 0),
            "time": time.strftime("%H:%M:%S"),
            "path": actual_output_path,
        }

    WORKER_TASKS.insert(0, {
        "task_id": task_id,
        "video_id": video_id,
        "chunk_filename": chunk_filename,
        "output_filename": output_filename or os.path.basename(actual_output_path),
        "resolution": resolution,
        "duration_ms": result.get("duration_ms", 0),
        "time": time.strftime("%H:%M:%S"),
        "status": "SUCCESS" if result.get("success") else "FAILED",
        "error": result.get("error"),
    })
    if len(WORKER_TASKS) > 25:
        WORKER_TASKS.pop()

    if result.get("success"):
        print(f"[{WORKER_ID}] ✨ Sukses selesai dalam {result.get('duration_ms', 0)} ms")
    else:
        print(f"[{WORKER_ID}] ❌ Gagal: {result.get('error')}")

    return jsonify(result), 200


@app.route("/", methods=["GET"])
def worker_dashboard_view():
    return render_template_string(
        HTML_WORKER_DASHBOARD,
        worker_id=WORKER_ID,
        port=WORKER_PORT,
        master_url=MASTER_NODE_URL,
    )


@app.route("/stream/chunk", methods=["GET"])
def stream_chunk_preview():
    if not LATEST_CHUNK_INFO or "path" not in LATEST_CHUNK_INFO:
        return jsonify({"error": "Belum ada chunk yang selesai diproses"}), 404
    path = LATEST_CHUNK_INFO["path"]
    if not os.path.exists(path):
        return jsonify({"error": "Berkas chunk fisik tidak ditemukan"}), 404
    return send_file(path, mimetype="video/mp4")


@app.route("/api/v1/worker/status", methods=["GET"])
def get_worker_status_api():
    has_chunk = bool(LATEST_CHUNK_INFO and "path" in LATEST_CHUNK_INFO and os.path.exists(LATEST_CHUNK_INFO["path"]))
    return jsonify({
        "worker_id": WORKER_ID,
        "port": WORKER_PORT,
        "master_url": MASTER_NODE_URL,
        "latest_chunk": LATEST_CHUNK_INFO if has_chunk else None,
        "tasks": WORKER_TASKS,
    })


def keep_registered_to_master(master_url: str, worker_id: str, port: int):
    registration_endpoint = f"{master_url.rstrip('/')}/api/v1/workers/register"
    local_ip = get_local_ip()
    payload = {
        "worker_id": worker_id,
        "instance_id": INSTANCE_ID,
        "url": f"http://{local_ip}:{port}",
        "port": port,
        "attributes": {
            "codec": "H.264/AAC",
            "gpu_accel": False,
            "max_concurrent": 2,
        },
    }

    first_attempt = True
    while True:
        try:
            response = requests.post(registration_endpoint, json=payload, timeout=3.5)
            if response.status_code == 200:
                if first_attempt:
                    print(f"[{worker_id}] ✅ Berhasil terhubung dan terdaftar ke Master ({master_url})")
                    first_attempt = False
            else:
                print(f"[{worker_id}] ⚠️ Gagal registrasi ke Master ({response.status_code}): {response.text}")
        except Exception as error:
            if first_attempt:
                print(f"[{worker_id}] ❌ Belum bisa terhubung ke Master di {registration_endpoint}")
                print(f"[{worker_id}]    Detail error: {error}")
                print(f"[{worker_id}]    (Mencoba kembali setiap 5 detik...)")
                first_attempt = False
        time.sleep(5)


def main():
    global WORKER_ID, WORKER_PORT, MASTER_NODE_URL

    ensure_firewall_allowed(ports=(5000, 5001))

    parser = argparse.ArgumentParser(description="DistriStream Transcoding Worker Node")
    parser.add_argument("--id", type=str, default=None, help="Symbolic Worker ID (e.g. Worker-1, Laptop-Teman)")
    parser.add_argument("--port", type=int, default=5001, help="Listening TCP Port")
    parser.add_argument("--master", type=str, default="", help="Master Node URL for multi-laptop demo (e.g. http://192.168.1.10:5000)")
    args = parser.parse_args()

    MASTER_NODE_URL = args.master

    WORKER_PORT = args.port
    if args.id:
        WORKER_ID = args.id
    else:
        worker_number = args.port - 5000 if args.port >= 5001 else args.port
        WORKER_ID = f"Worker-{worker_number}"

    local_lan_ip = get_local_ip()

    print("=" * 65)
    print(f"  DistriStream Worker Node : {WORKER_ID}")
    print(f"  Listening Local Address  : http://localhost:{WORKER_PORT}")
    print(f"  Listening Network Address: http://{local_lan_ip}:{WORKER_PORT}")
    if args.master:
        print(f"  Connected to Master Node : {args.master}")
        registration_thread = threading.Thread(
            target=keep_registered_to_master,
            args=(args.master, WORKER_ID, WORKER_PORT),
            daemon=True,
        )
        registration_thread.start()
    print("=" * 65)

    app.run(host="0.0.0.0", port=WORKER_PORT, debug=False)


if __name__ == "__main__":
    main()
