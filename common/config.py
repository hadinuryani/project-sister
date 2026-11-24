import os
import shutil
import socket
import subprocess
from pathlib import Path
from typing import Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BASE_DIR = str(PROJECT_ROOT)
STORAGE_DIR = str(PROJECT_ROOT / "storage")
RAW_DIR = str(PROJECT_ROOT / "storage" / "raw_uploads")
CHUNKS_DIR = str(PROJECT_ROOT / "storage" / "chunks")
OUTPUT_DIR = str(PROJECT_ROOT / "storage" / "output_hls")

for folder in (STORAGE_DIR, RAW_DIR, CHUNKS_DIR, OUTPUT_DIR):
    os.makedirs(folder, exist_ok=True)

MASTER_REST_PORT = 5000
DEFAULT_WORKER_PORTS = [5001, 5002, 5003, 5004]
DEFAULT_WORKER_URLS = [f"http://127.0.0.1:{port}" for port in DEFAULT_WORKER_PORTS]

DEFAULT_CHUNK_DURATION_SECONDS = 5

RESOLUTIONS: Dict[str, Dict[str, str]] = {
    "720p": {"scale": "1280:720", "video_bitrate": "1500k", "audio_bitrate": "128k"},
    "480p": {"scale": "854:480", "video_bitrate": "800k", "audio_bitrate": "96k"},
    "360p": {"scale": "640:360", "video_bitrate": "400k", "audio_bitrate": "64k"},
}


def get_local_ip() -> str:
    """Mendeteksi alamat IP LAN/Wi-Fi aktif untuk demonstrasi kluster multi-laptop."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except Exception:
        return "127.0.0.1"


def get_ffmpeg_binary() -> Optional[str]:
    try:
        import imageio_ffmpeg

        executable = imageio_ffmpeg.get_ffmpeg_exe()
        if os.path.exists(executable):
            return executable
    except ImportError:
        pass

    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg:
        return system_ffmpeg

    try:
        check = subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if check.returncode == 0:
            return "ffmpeg"
    except Exception:
        pass

    return None


def ensure_firewall_allowed(ports=(5000, 5001)) -> bool:
    """Otomatis memeriksa dan mendaftarkan port ke Windows Firewall melalui kode Python."""
    if os.name != "nt":
        return True

    try:
        check = subprocess.run(
            ["netsh", "advfirewall", "firewall", "show", "rule", "name=DistriStream Engine"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if check.returncode == 0:
            return True

        import ctypes
        command = 'netsh advfirewall firewall add rule name="DistriStream Engine" dir=in action=allow protocol=TCP localport=5000-5010'
        result = ctypes.windll.shell32.ShellExecuteW(None, "runas", "cmd.exe", f"/c {command}", None, 0)
        return result > 32
    except Exception:
        return False
