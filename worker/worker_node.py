import argparse
import os
import sys
import threading
import time
import uuid
from typing import Any, Dict

from flask import Flask, jsonify, request
import requests

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common.config import get_local_ip
from worker.transcoder import transcode_video_chunk

app = Flask(__name__)

PROCESSED_REQUESTS: Dict[str, Dict[str, Any]] = {}

INSTANCE_ID = str(uuid.uuid4())[:8]
WORKER_ID = "Worker-1"
WORKER_PORT = 5001


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
    is_remote_mode = (not os.path.exists(input_path)) and bool(master_url and chunk_filename and video_id)
    actual_input_path = input_path
    actual_output_path = output_path

    if is_remote_mode:
        local_chunk_dir = os.path.join(ROOT_DIR, "storage", "chunks", video_id)
        os.makedirs(local_chunk_dir, exist_ok=True)
        actual_input_path = os.path.join(local_chunk_dir, chunk_filename)
        actual_output_path = os.path.join(local_chunk_dir, output_filename or f"{chunk_filename}_{resolution}.mp4")

        download_endpoint = f"{master_url}/api/v1/chunks/{video_id}/{chunk_filename}"
        print(f"[{WORKER_ID}] Mengunduh chunk jaringan dari Master: {chunk_filename}...")
        try:
            download_response = requests.get(download_endpoint, timeout=30)
            if download_response.status_code != 200:
                return jsonify({"success": False, "error": f"Gagal mengunduh chunk dari Master: {download_response.status_code}"}), 500
            with open(actual_input_path, "wb") as chunk_file:
                chunk_file.write(download_response.content)
        except Exception as error:
            return jsonify({"success": False, "error": f"Koneksi unduh chunk ke Master gagal: {error}"}), 500

    # Eksekusi transcoding lokal menggunakan CPU/GPU laptop ini
    result = transcode_video_chunk(actual_input_path, actual_output_path, target_resolution=resolution)
    result["worker_id"] = WORKER_ID
    result["task_id"] = task_id
    result["seq_num"] = sequence_number

    # Jika mode multi-laptop, kirim kembali chunk yang sudah selesai ke Master
    if is_remote_mode and result["success"]:
        upload_endpoint = f"{master_url}/api/v1/chunks/{video_id}/{output_filename}"
        print(f"[{WORKER_ID}] Mengirim balik hasil transcode ke Master: {output_filename}...")
        try:
            with open(actual_output_path, "rb") as transcoded_file:
                files_payload = {"chunk_file": (output_filename, transcoded_file, "video/mp4")}
                upload_response = requests.post(upload_endpoint, files=files_payload, timeout=30)
                if upload_response.status_code != 200:
                    return jsonify({"success": False, "error": "Gagal mengunggah chunk hasil ke Master"}), 500
        except Exception as error:
            return jsonify({"success": False, "error": f"Koneksi kirim balik ke Master gagal: {error}"}), 500

    PROCESSED_REQUESTS[cache_key] = result

    if result["success"]:
        print(f"[{WORKER_ID}] Sukses selesai dalam {result['duration_ms']} ms")
    else:
        print(f"[{WORKER_ID}] Gagal: {result.get('error')}")

    return jsonify(result), 200


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
    global WORKER_ID, WORKER_PORT

    parser = argparse.ArgumentParser(description="DistriStream Transcoding Worker Node")
    parser.add_argument("--id", type=str, default=None, help="Symbolic Worker ID (e.g. Worker-1, Laptop-Teman)")
    parser.add_argument("--port", type=int, default=5001, help="Listening TCP Port")
    parser.add_argument("--master", type=str, default="", help="Master Node URL for multi-laptop demo (e.g. http://192.168.1.10:5000)")
    args = parser.parse_args()

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
