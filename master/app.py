import os
import shutil
import sys
import threading
import time
import uuid
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template_string, request, send_file

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common.config import CHUNKS_DIR, MASTER_REST_PORT, OUTPUT_DIR, RAW_DIR, get_local_ip
from master.scheduler import MasterScheduler
from master.segmenter import split_video_into_chunks

app = Flask(__name__)
scheduler = MasterScheduler()

JOBS: Dict[str, Any] = {}
SYSTEM_LOGS: List[Dict[str, str]] = [
    {
        "time": time.strftime("%H:%M:%S"),
        "sender": "MASTER",
        "type": "info",
        "msg": "DistriStream cluster initialized successfully.",
    }
]


def add_log(sender: str, msg: str, log_type: str = "info"):
    SYSTEM_LOGS.append({
        "time": time.strftime("%H:%M:%S"),
        "sender": sender,
        "type": log_type,
        "msg": msg,
    })
    if len(SYSTEM_LOGS) > 40:
        SYSTEM_LOGS.pop(0)


HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DistriStream Engine — Studio Transcoding Terdistribusi</title>
    <!-- Google Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;700;800;900&family=JetBrains+Mono:wght@600;700&display=swap" rel="stylesheet">

    <style>
        :root {
            --bg-canvas: #FAF8F0;
            --bg-grid: #E8E5D8;
            --surface: #FFFFFF;
            --surface-soft: #FFFDF8;
            --border: #000000;
            --border-width: 3px;
            --shadow-hard: 4px 4px 0px #000000;
            --shadow-lg: 6px 6px 0px #000000;
            --shadow-sm: 2px 2px 0px #000000;
            
            /* Neo-Brutalism Cerah Palette */
            --yellow-main: #FFE600;
            --yellow-light: #FFF9C4;
            --green-neo: #00E599;
            --green-soft: #DCFCE7;
            --blue-neo: #38BDF8;
            --blue-soft: #E0F2FE;
            --coral-neo: #FF5A5F;
            --purple-neo: #D8B4FE;
            --purple-soft: #F3E8FF;
            --text-dark: #000000;
            --text-muted: #4B5563;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Plus Jakarta Sans', sans-serif;
            color: var(--text-dark);
        }

        body {
            background-color: var(--bg-canvas);
            background-image: 
                radial-gradient(var(--bg-grid) 2px, transparent 2px),
                radial-gradient(var(--bg-grid) 2px, var(--bg-canvas) 2px);
            background-size: 32px 32px;
            background-position: 0 0, 16px 16px;
            min-height: 100vh;
            padding: 2rem 1.25rem 4rem 1.25rem;
        }

        .container {
            max-width: 1160px;
            margin: 0 auto;
        }

        /* Top Header */
        header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 1.25rem 1.5rem;
            margin-bottom: 2rem;
            background: var(--surface);
            border: var(--border-width) solid var(--border);
            border-radius: 16px;
            box-shadow: var(--shadow-lg);
            flex-wrap: wrap;
            gap: 1.25rem;
        }

        .brand-group {
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .brand-avatar {
            width: 54px;
            height: 54px;
            background: var(--yellow-main);
            border: var(--border-width) solid var(--border);
            border-radius: 12px;
            box-shadow: var(--shadow-sm);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.75rem;
            font-weight: 900;
        }

        .brand-title h1 {
            font-size: 1.75rem;
            font-weight: 900;
            letter-spacing: -0.5px;
            text-transform: uppercase;
            line-height: 1.1;
        }

        .brand-title p {
            font-size: 0.85rem;
            font-weight: 700;
            color: var(--text-muted);
            margin-top: 0.25rem;
        }

        .cluster-pill {
            background: var(--green-neo);
            border: var(--border-width) solid var(--border);
            border-radius: 999px;
            box-shadow: var(--shadow-hard);
            padding: 0.6rem 1.25rem;
            font-size: 0.88rem;
            font-weight: 900;
            letter-spacing: 0.5px;
            display: flex;
            align-items: center;
            gap: 0.6rem;
            text-transform: uppercase;
        }

        .blink-dot {
            width: 12px;
            height: 12px;
            background: #000;
            border-radius: 50%;
            display: inline-block;
            animation: pulse-dot 1.2s infinite ease-in-out;
        }

        @keyframes pulse-dot {
            0%, 100% { transform: scale(1); opacity: 1; }
            50% { transform: scale(1.35); opacity: 0.5; }
        }

        /* Generic Neo-Brutalism Card */
        .card {
            background: var(--surface);
            border: var(--border-width) solid var(--border);
            border-radius: 16px;
            box-shadow: var(--shadow-hard);
            padding: 1.5rem;
            margin-bottom: 2rem;
            position: relative;
        }

        .card-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1.25rem;
            flex-wrap: wrap;
            gap: 0.75rem;
        }

        .badge-title {
            background: var(--yellow-main);
            border: 2px solid var(--border);
            border-radius: 8px;
            box-shadow: var(--shadow-sm);
            padding: 0.35rem 0.85rem;
            font-size: 0.95rem;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 0.3px;
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
        }

        .btn-neo {
            background: var(--surface);
            border: 2px solid var(--border);
            border-radius: 8px;
            box-shadow: var(--shadow-sm);
            font-weight: 800;
            font-size: 0.82rem;
            padding: 0.45rem 0.9rem;
            cursor: pointer;
            transition: all 0.1s ease;
            text-transform: uppercase;
        }

        .btn-neo:hover {
            transform: translate(-2px, -2px);
            box-shadow: 4px 4px 0px #000;
        }

        .btn-neo:active {
            transform: translate(2px, 2px);
            box-shadow: 0px 0px 0px #000;
        }

        /* Worker Grid */
        .worker-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 1.25rem;
        }

        .worker-box {
            background: var(--green-soft);
            border: var(--border-width) solid var(--border);
            border-radius: 12px;
            box-shadow: var(--shadow-sm);
            padding: 1.15rem;
            transition: all 0.12s ease;
        }

        .worker-box:hover {
            transform: translate(-2px, -2px);
            box-shadow: var(--shadow-hard);
        }

        .worker-box-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.75rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px dashed #000;
        }

        .worker-name {
            font-size: 1.05rem;
            font-weight: 900;
            letter-spacing: -0.3px;
        }

        .worker-status-badge {
            background: var(--green-neo);
            border: 2px solid var(--border);
            border-radius: 6px;
            box-shadow: 1.5px 1.5px 0px #000;
            font-size: 0.7rem;
            font-weight: 900;
            padding: 0.2rem 0.55rem;
        }

        .worker-specs {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.78rem;
            font-weight: 700;
            display: flex;
            flex-direction: column;
            gap: 0.35rem;
        }

        .worker-specs span {
            font-family: inherit;
        }

        /* Two-Column Studio Layout */
        .studio-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
            align-items: stretch;
        }

        @media (max-width: 900px) {
            .studio-grid {
                grid-template-columns: 1fr;
            }
        }

        /* Dropzone Upload */
        .dropzone-neo {
            border: 3px dashed var(--border);
            border-radius: 14px;
            background: var(--yellow-light);
            padding: 2.25rem 1.5rem;
            text-align: center;
            cursor: pointer;
            position: relative;
            transition: all 0.12s ease;
            margin-bottom: 1.25rem;
        }

        .dropzone-neo:hover {
            background: #FFF382;
            transform: scale(1.01);
        }

        .dropzone-neo input[type="file"] {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            opacity: 0;
            cursor: pointer;
        }

        .drop-icon-box {
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
        }

        .drop-main-text {
            font-size: 1.05rem;
            font-weight: 900;
            margin-bottom: 0.25rem;
        }

        .drop-sub-text {
            font-size: 0.8rem;
            font-weight: 700;
            color: var(--text-muted);
        }

        /* Resolution Selector */
        .res-label {
            font-size: 0.82rem;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 0.6rem;
            display: block;
        }

        .res-selector-group {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 0.75rem;
            margin-bottom: 1.5rem;
        }

        .res-tile {
            background: var(--surface);
            border: 2.5px solid var(--border);
            border-radius: 10px;
            box-shadow: var(--shadow-sm);
            padding: 0.85rem 0.5rem;
            text-align: center;
            cursor: pointer;
            transition: all 0.12s ease;
        }

        .res-tile:hover {
            transform: translate(-2px, -2px);
            box-shadow: var(--shadow-hard);
        }

        .res-tile.active {
            background: var(--yellow-main);
            box-shadow: var(--shadow-hard);
            transform: translate(1px, 1px);
        }

        .res-title {
            font-size: 1.05rem;
            font-weight: 900;
        }

        .res-bitrate {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.75rem;
            font-weight: 700;
            margin-top: 0.2rem;
            color: var(--text-muted);
        }

        .res-tile.active .res-bitrate {
            color: #000;
        }

        /* Action Button */
        .btn-launch-big {
            width: 100%;
            background: var(--yellow-main);
            color: #000;
            border: var(--border-width) solid var(--border);
            border-radius: 12px;
            box-shadow: var(--shadow-hard);
            padding: 1.15rem;
            font-size: 1.15rem;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            cursor: pointer;
            transition: all 0.12s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.6rem;
        }

        .btn-launch-big:hover:not(:disabled) {
            transform: translate(-3px, -3px);
            box-shadow: var(--shadow-lg);
            background: #FFED4A;
        }

        .btn-launch-big:active:not(:disabled) {
            transform: translate(3px, 3px);
            box-shadow: 1px 1px 0px #000;
        }

        .btn-launch-big:disabled {
            background: #E5E7EB;
            color: #9CA3AF;
            cursor: not-allowed;
            box-shadow: var(--shadow-sm);
        }

        /* Progress Pipeline */
        .pipeline-card {
            display: none;
            margin-top: 1.5rem;
            background: var(--surface-soft);
            border: 2.5px solid var(--border);
            border-radius: 12px;
            padding: 1.15rem;
            box-shadow: var(--shadow-sm);
        }

        .pipeline-info {
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-weight: 900;
            margin-bottom: 0.6rem;
            font-size: 0.9rem;
        }

        .pipeline-pct {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.1rem;
            background: var(--yellow-main);
            padding: 0.2rem 0.6rem;
            border: 2px solid var(--border);
            border-radius: 6px;
        }

        .track-neo {
            height: 28px;
            background: var(--surface);
            border: var(--border-width) solid var(--border);
            border-radius: 999px;
            overflow: hidden;
            box-shadow: 2px 2px 0px #000;
        }

        .fill-neo {
            height: 100%;
            width: 0%;
            background: var(--green-neo);
            border-right: var(--border-width) solid var(--border);
            transition: width 0.35s ease;
        }

        /* Terminal Window */
        .terminal-window {
            border: var(--border-width) solid var(--border);
            border-radius: 16px;
            box-shadow: var(--shadow-hard);
            background: #18181B;
            overflow: hidden;
            display: flex;
            flex-direction: column;
            height: 100%;
            min-height: 480px;
        }

        .terminal-topbar {
            background: var(--yellow-main);
            border-bottom: var(--border-width) solid var(--border);
            padding: 0.85rem 1.15rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .window-dots {
            display: flex;
            gap: 7px;
        }

        .dot-circle {
            width: 14px;
            height: 14px;
            border-radius: 50%;
            border: 2px solid #000;
            display: inline-block;
        }

        .dot-red { background: #FF5F56; }
        .dot-yellow { background: #FFBD2E; }
        .dot-green { background: #27C93F; }

        .terminal-header-title {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.82rem;
            font-weight: 900;
            letter-spacing: 0.5px;
            text-transform: uppercase;
        }

        .terminal-console {
            padding: 1.15rem;
            flex-grow: 1;
            overflow-y: auto;
            max-height: 440px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.82rem;
            line-height: 1.6;
            background: #18181B;
        }

        .log-entry {
            margin-bottom: 0.4rem;
            word-break: break-all;
            display: flex;
            gap: 0.6rem;
            font-family: inherit;
        }

        .log-time {
            color: #71717A;
            font-family: inherit;
            flex-shrink: 0;
        }

        .log-source {
            font-weight: 700;
            font-family: inherit;
            flex-shrink: 0;
        }

        .log-source.MASTER { color: var(--yellow-main); }
        .log-source.GATEWAY { color: var(--blue-neo); }
        .log-source.SEGMENTER { color: var(--purple-neo); }
        .log-source.GATHER { color: var(--green-neo); }
        .log-source.DISCOVERY { color: #F472B6; }

        .log-message {
            color: #F4F4F5;
            font-family: inherit;
        }

        /* Video Showcase */
        .showcase-neo {
            display: none;
            background: var(--surface);
            border: var(--border-width) solid var(--border);
            border-radius: 16px;
            box-shadow: var(--shadow-lg);
            padding: 1.75rem;
            margin-top: 2rem;
        }

        .metrics-grid-neo {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }

        .metric-card-neo {
            border: 2px solid var(--border);
            border-radius: 10px;
            box-shadow: var(--shadow-sm);
            padding: 0.85rem 1rem;
            text-align: center;
        }

        .metric-card-neo:nth-child(1) { background: var(--yellow-light); }
        .metric-card-neo:nth-child(2) { background: var(--green-soft); }
        .metric-card-neo:nth-child(3) { background: var(--blue-soft); }
        .metric-card-neo:nth-child(4) { background: var(--purple-soft); }

        .metric-title {
            font-size: 0.75rem;
            font-weight: 800;
            text-transform: uppercase;
            color: var(--text-muted);
            margin-bottom: 0.2rem;
        }

        .metric-number {
            font-size: 1.35rem;
            font-weight: 900;
            letter-spacing: -0.5px;
        }

        .video-box-neo {
            width: 100%;
            background: #000;
            border: var(--border-width) solid var(--border);
            border-radius: 12px;
            box-shadow: var(--shadow-hard);
            max-height: 480px;
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Top Navigation -->
        <header>
            <div class="brand-group">
                <div class="brand-avatar">⚡</div>
                <div class="brand-title">
                    <h1>DistriStream Engine</h1>
                    <p>Platform Transcoding Video Paralel & Rendering Terdistribusi</p>
                </div>
            </div>
            <div class="cluster-pill" id="clusterStatusPill">
                <span class="blink-dot"></span>
                <span id="clusterStatusText">Mendeteksi Kluster...</span>
            </div>
        </header>

        <!-- Active Worker Nodes Card -->
        <div class="card">
            <div class="card-header">
                <div class="badge-title">🖥️ Active Cluster Worker Nodes (RPC / MPI Ring)</div>
                <button class="btn-neo" onclick="refreshWorkers()">🔄 Refresh Topology</button>
            </div>
            <div id="workerGrid" class="worker-grid">
                <!-- Injected via JavaScript -->
            </div>
        </div>

        <!-- Main Studio Workspace -->
        <div class="studio-grid">
            <!-- Left: Job Dispatcher -->
            <div class="card" style="margin-bottom: 0;">
                <div class="card-header">
                    <div class="badge-title" style="background: var(--blue-neo);">📤 Studio Upload & Job Dispatcher</div>
                </div>

                <form id="uploadForm">
                    <div class="dropzone-neo" id="dropzoneBox">
                        <input type="file" id="videoFile" accept="video/*" required>
                        <div class="drop-icon-box">🎬</div>
                        <div class="drop-main-text" id="dropMainLabel">Pilih atau Drag & Drop Berkas Video</div>
                        <div class="drop-sub-text">Mendukung MP4, MKV, MOV (Contoh: sample_test_video.mp4)</div>
                    </div>

                    <label class="res-label">Pilih Target Resolusi Encoding:</label>
                    <div class="res-selector-group">
                        <div class="res-tile active" onclick="selectRes('720p', this)">
                            <div class="res-title">720p HD</div>
                            <div class="res-bitrate">1500 kbps</div>
                        </div>
                        <div class="res-tile" onclick="selectRes('480p', this)">
                            <div class="res-title">480p SD</div>
                            <div class="res-bitrate">800 kbps</div>
                        </div>
                        <div class="res-tile" onclick="selectRes('360p', this)">
                            <div class="res-title">360p Low</div>
                            <div class="res-bitrate">400 kbps</div>
                        </div>
                    </div>

                    <button type="submit" id="btnLaunch" class="btn-launch-big">
                        <span>⚡</span> Mulai Transcoding Paralel
                    </button>
                </form>

                <!-- Pipeline Progress -->
                <div id="pipelineStatus" class="pipeline-card">
                    <div class="pipeline-info">
                        <span id="pipelinePhaseText">Memulai pemotongan video...</span>
                        <span id="progressPercent" class="pipeline-pct">0%</span>
                    </div>
                    <div class="track-neo">
                        <div id="progressFill" class="fill-neo"></div>
                    </div>

                    <!-- Visualisasi Chunks Scatter-Gather -->
                    <div style="margin-top: 1.25rem; border-top: 2.5px dashed var(--border); padding-top: 1rem;">
                        <div style="font-weight: 900; font-size: 0.82rem; text-transform: uppercase; margin-bottom: 0.75rem; display: flex; align-items: center; justify-content: space-between;">
                            <span>🧩 Visualisasi Segmen & Distribusi Worker:</span>
                            <span id="chunksCountBadge" style="font-size: 0.72rem; font-weight: 800; background: var(--yellow-main); padding: 0.2rem 0.55rem; border: 2px solid #000; border-radius: 6px;">0 Segmen</span>
                        </div>
                        <div id="chunksGrid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 0.65rem;">
                            <!-- Dynamic Chunk Cards -->
                        </div>
                    </div>
                </div>
            </div>

            <!-- Right: Realtime Event Terminal -->
            <div class="terminal-window">
                <div class="terminal-topbar">
                    <div class="window-dots">
                        <span class="dot-circle dot-red"></span>
                        <span class="dot-circle dot-yellow"></span>
                        <span class="dot-circle dot-green"></span>
                    </div>
                    <div class="terminal-header-title">📡 Live Cluster RPC Telemetry</div>
                    <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; font-weight: 700;">TCP STREAM</div>
                </div>
                <div class="terminal-console" id="terminalLogs">
                    <!-- Logs injected via JS -->
                </div>
            </div>
        </div>

        <!-- Video Player Showcase -->
        <div id="showcaseCard" class="showcase-neo">
            <div class="card-header">
                <div class="badge-title" style="background: var(--green-neo);">🎉 Transcoding Sukses — Video Siap Diputar</div>
                <a id="btnDownload" href="#" download class="btn-neo" style="background: var(--blue-neo); text-decoration: none;">
                    ⬇️ Download Video
                </a>
            </div>

            <div class="metrics-grid-neo">
                <div class="metric-card-neo">
                    <div class="metric-title">Target Resolusi</div>
                    <div class="metric-number" id="metricRes">-</div>
                </div>
                <div class="metric-card-neo">
                    <div class="metric-title">Waktu Pemrosesan</div>
                    <div class="metric-number" id="metricTime">-</div>
                </div>
                <div class="metric-card-neo">
                    <div class="metric-title">Worker Terlibat</div>
                    <div class="metric-number" id="metricWorkers">-</div>
                </div>
                <div class="metric-card-neo">
                    <div class="metric-title">Jumlah Segmen</div>
                    <div class="metric-number" id="metricChunks">-</div>
                </div>
            </div>

            <video id="videoPlayer" class="video-box-neo" controls></video>
        </div>
    </div>

    <script>
        let selectedResolution = '720p';

        function selectRes(res, element) {
            selectedResolution = res;
            document.querySelectorAll('.res-tile').forEach(tile => tile.classList.remove('active'));
            element.classList.add('active');
        }

        // File selection label updater
        document.getElementById('videoFile').addEventListener('change', function() {
            if (this.files && this.files[0]) {
                const fileName = this.files[0].name;
                const fileSizeMB = (this.files[0].size / (1024 * 1024)).toFixed(1);
                document.getElementById('dropMainLabel').innerText = `${fileName} (${fileSizeMB} MB)`;
            }
        });

        // Worker nodes poller
        async function refreshWorkers() {
            try {
                const response = await fetch('/api/v1/workers');
                const data = await response.json();
                const grid = document.getElementById('workerGrid');
                const pillText = document.getElementById('clusterStatusText');
                const pill = document.getElementById('clusterStatusPill');

                if (!data.workers || data.workers.length === 0) {
                    pillText.innerText = '0 Worker Aktif';
                    pill.style.background = '#FF5A5F';
                    grid.innerHTML = '<div style="font-weight: 800; padding: 1rem; color: #DC2626;">⚠️ Tidak ada Worker terdeteksi. Jalankan <code>python worker/worker_node.py</code> di terminal.</div>';
                } else {
                    pillText.innerText = `${data.workers.length} Worker Nodes Aktif`;
                    pill.style.background = 'var(--green-neo)';

                    grid.innerHTML = data.workers.map(worker => `
                        <div class="worker-box">
                            <div class="worker-box-header">
                                <span class="worker-name">${worker.worker_id}</span>
                                <span class="worker-status-badge">ONLINE</span>
                            </div>
                            <div class="worker-specs">
                                <div>URI  : <span>${worker.url}</span></div>
                                <div>CODEC: <span>${worker.attributes.codec || 'H.264/AAC'}</span></div>
                                <div>ACCEL: <span>${worker.attributes.gpu_accel ? 'GPU Hardware' : 'CPU Software'}</span></div>
                            </div>
                        </div>
                    `).join('');
                }
            } catch (err) {
                console.error('Error fetching workers:', err);
            }
        }

        // Realtime logs poller
        async function fetchLogs() {
            try {
                const response = await fetch('/api/v1/logs');
                const data = await response.json();
                const consoleBox = document.getElementById('terminalLogs');

                consoleBox.innerHTML = data.logs.map(log => `
                    <div class="log-entry">
                        <span class="log-time">[${log.time}]</span>
                        <span class="log-source ${log.sender}">${log.sender}:</span>
                        <span class="log-message">${log.msg}</span>
                    </div>
                `).join('');

                consoleBox.scrollTop = consoleBox.scrollHeight;
            } catch (err) {}
        }

        refreshWorkers();
        fetchLogs();
        setInterval(refreshWorkers, 4000);
        setInterval(fetchLogs, 1500);

        // Upload Form handler
        document.getElementById('uploadForm').onsubmit = async (event) => {
            event.preventDefault();
            const fileInput = document.getElementById('videoFile');
            const submitBtn = document.getElementById('btnLaunch');

            if (!fileInput.files[0]) return;

            submitBtn.disabled = true;
            submitBtn.innerHTML = '<span>⏳</span> Memproses di Kluster...';

            const pipelineCard = document.getElementById('pipelineStatus');
            const progressFill = document.getElementById('progressFill');
            const progressPercent = document.getElementById('progressPercent');
            const phaseText = document.getElementById('pipelinePhaseText');
            pipelineCard.style.display = 'block';

            const formData = new FormData();
            formData.append('video', fileInput.files[0]);
            formData.append('resolution', selectedResolution);

            try {
                const response = await fetch('/api/v1/videos/upload', {
                    method: 'POST',
                    body: formData,
                });
                const result = await response.json();

                if (!result.success) {
                    alert('Gagal: ' + result.error);
                    submitBtn.disabled = false;
                    submitBtn.innerHTML = '<span>⚡</span> Mulai Transcoding Paralel';
                    return;
                }

                const videoId = result.video_id;
                phaseText.innerText = `Segmentasi selesai (${result.chunks_count} chunks). Menyebarkan via MPI...`;

                const poller = setInterval(async () => {
                    const statusResponse = await fetch('/api/v1/videos/' + videoId);
                    const statusData = await statusResponse.json();

                    progressFill.style.width = statusData.progress + '%';
                    progressPercent.innerText = statusData.progress + '%';
                    phaseText.innerText = statusData.status;

                    if (statusData.chunks && statusData.chunks.length > 0) {
                        const badgeEl = document.getElementById('chunksCountBadge');
                        if (badgeEl) badgeEl.innerText = `${statusData.chunks.length} Segmen (5s/chunk)`;
                        const chunksGrid = document.getElementById('chunksGrid');
                        if (chunksGrid) {
                            chunksGrid.innerHTML = statusData.chunks.map(chunk => {
                                let badgeBg = 'var(--yellow-main)';
                                let badgeText = '⏳ MENUNGGU';
                                if (chunk.status === 'DISPATCHED') {
                                    badgeBg = 'var(--blue-neo)';
                                    badgeText = '⚡ PROCESSING';
                                } else if (chunk.status === 'DONE') {
                                    badgeBg = 'var(--green-neo)';
                                    badgeText = '✅ SELESAI';
                                } else if (chunk.status === 'FAILOVER') {
                                    badgeBg = 'var(--coral-neo)';
                                    badgeText = '🔄 FAILOVER';
                                }

                                return `
                                    <div style="background: #FFFFFF; border: 2.5px solid #000; border-radius: 8px; padding: 0.6rem; box-shadow: 2px 2px 0px #000;">
                                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.35rem;">
                                            <span style="font-weight: 900; font-size: 0.8rem;">🎬 Segmen #${chunk.index + 1}</span>
                                            <span style="font-size: 0.65rem; font-weight: 900; padding: 0.1rem 0.4rem; border: 1.5px solid #000; border-radius: 5px; background: ${badgeBg};">${badgeText}</span>
                                        </div>
                                        <div style="font-size: 0.7rem; font-family: 'JetBrains Mono', monospace; color: var(--text-muted);">${chunk.filename}</div>
                                        <div style="font-size: 0.76rem; font-weight: 800; margin-top: 0.35rem; display: flex; align-items: center; gap: 0.3rem;">
                                            <span>💻</span> <span style="color: #2563EB;">${chunk.worker || 'Menunggu Node...'}</span>
                                        </div>
                                    </div>
                                `;
                            }).join('');
                        }
                    }

                    if (statusData.status === 'COMPLETED') {
                        clearInterval(poller);
                        submitBtn.disabled = false;
                        submitBtn.innerHTML = '<span>⚡</span> Mulai Transcoding Paralel';

                        const showcase = document.getElementById('showcaseCard');
                        showcase.style.display = 'block';
                        document.getElementById('metricRes').innerText = statusData.resolution;
                        document.getElementById('metricTime').innerText = (statusData.total_time_ms / 1000).toFixed(2) + 's';
                        document.getElementById('metricWorkers').innerText = statusData.workers_used + ' Nodes';
                        document.getElementById('metricChunks').innerText = statusData.chunks_count + ' Segmen';

                        const streamUrl = '/api/v1/videos/' + videoId + '/stream';
                        document.getElementById('videoPlayer').src = streamUrl;
                        document.getElementById('btnDownload').href = streamUrl;

                        showcase.scrollIntoView({ behavior: 'smooth' });
                    } else if (statusData.status === 'FAILED') {
                        clearInterval(poller);
                        alert('Transcoding Gagal: ' + statusData.error);
                        submitBtn.disabled = false;
                        submitBtn.innerHTML = '<span>⚡</span> Mulai Transcoding Paralel';
                    }
                }, 1000);

            } catch (err) {
                alert('Kesalahan jaringan: ' + err);
                submitBtn.disabled = false;
                submitBtn.innerHTML = '<span>⚡</span> Mulai Transcoding Paralel';
            }
        };
    </script>
</body>
</html>"""


@app.route("/", methods=["GET"])
def index():
    return render_template_string(HTML_DASHBOARD)


@app.route("/api/v1/logs", methods=["GET"])
def get_logs():
    return jsonify({"logs": SYSTEM_LOGS})


@app.route("/api/v1/workers", methods=["GET"])
def get_workers():
    active = scheduler.discover_workers()
    return jsonify({"workers": active})


@app.route("/api/v1/workers/register", methods=["POST"])
def register_remote_worker():
    payload = request.get_json()
    if not payload or "url" not in payload:
        return jsonify({"success": False, "error": "Invalid registration payload"}), 400

    worker_port = payload.get("port", 5001)
    remote_ip = request.remote_addr

    # Jika registrasi datang dari laptop lain, selalu gunakan IP pengirim yang sebenarnya
    if remote_ip and remote_ip not in ("127.0.0.1", "localhost", "::1"):
        worker_url = f"http://{remote_ip}:{worker_port}"
    else:
        worker_url = payload["url"]

    worker_name = payload.get("worker_id", worker_url)
    instance_id = payload.get("instance_id", worker_url)
    attributes = payload.get("attributes", {})

    scheduler.register_worker_heartbeat(
        url=worker_url,
        worker_id=worker_name,
        instance_id=instance_id,
        attributes=attributes,
    )
    add_log("DISCOVERY", f"Node pekerja terhubung: {worker_name} @ {worker_url}", "info")
    return jsonify({"success": True, "registered_url": worker_url}), 200


@app.route("/api/v1/chunks/<video_id>/<chunk_filename>", methods=["GET"])
def download_chunk_for_worker(video_id: str, chunk_filename: str):
    chunk_path = os.path.join(CHUNKS_DIR, video_id, chunk_filename)
    if not os.path.exists(chunk_path):
        return jsonify({"error": "Chunk not found"}), 404
    return send_file(chunk_path, mimetype="video/mp4")


@app.route("/api/v1/chunks/<video_id>/<chunk_filename>", methods=["POST"])
def receive_transcoded_chunk(video_id: str, chunk_filename: str):
    if "chunk_file" not in request.files:
        return jsonify({"success": False, "error": "Missing chunk_file payload"}), 400

    uploaded = request.files["chunk_file"]
    target_dir = os.path.join(CHUNKS_DIR, video_id)
    os.makedirs(target_dir, exist_ok=True)
    destination = os.path.join(target_dir, chunk_filename)
    uploaded.save(destination)
    return jsonify({"success": True, "saved": destination}), 200


def background_transcode(video_id: str, chunks: List[str], resolution: str):
    def update_progress(vid: str, pct: int, comp: int, tot: int):
        if vid in JOBS:
            JOBS[vid]["progress"] = pct
            JOBS[vid]["status"] = f"Parallel Encoding: Chunk {comp}/{tot} selesai ({pct}%)"
            add_log("MASTER", f"Parallel progress: Chunk {comp}/{tot} ({pct}%)")

    def update_chunk(vid: str, idx: int, status: str, worker: str):
        if vid in JOBS and "chunks" in JOBS[vid] and idx < len(JOBS[vid]["chunks"]):
            JOBS[vid]["chunks"][idx]["status"] = status
            JOBS[vid]["chunks"][idx]["worker"] = worker
            add_log("MASTER", f"Segmen #{idx + 1} ({os.path.basename(chunks[idx])}) -> {worker} [{status}]")

    try:
        add_log("MASTER", f"Memulai Scatter {len(chunks)} chunks ke worker nodes...")
        result = scheduler.dispatch_parallel_transcode(
            video_id=video_id,
            chunks=chunks,
            target_resolution=resolution,
            progress_callback=update_progress,
            chunk_callback=update_chunk,
        )
        JOBS[video_id]["status"] = "COMPLETED"
        JOBS[video_id]["progress"] = 100
        JOBS[video_id]["output_file"] = result["output_file"]
        JOBS[video_id]["total_time_ms"] = result["total_time_ms"]
        JOBS[video_id]["workers_used"] = result["workers_used"]
        add_log("GATHER", f"Gathering selesai. Video utuh tersimpan ({result['total_time_ms']/1000:.2f}s).", "success")
    except Exception as error:
        JOBS[video_id]["status"] = "FAILED"
        JOBS[video_id]["error"] = str(error)
        add_log("MASTER", f"Transcode error: {error}", "error")


@app.route("/api/v1/videos/upload", methods=["POST"])
def upload_video():
    if "video" not in request.files:
        return jsonify({"success": False, "error": "Berkas video belum disertakan"}), 400

    file = request.files["video"]
    if not file or not file.filename:
        return jsonify({"success": False, "error": "Silakan pilih berkas video terlebih dahulu"}), 400

    resolution = request.form.get("resolution", "720p")
    video_id = str(uuid.uuid4())[:8]

    raw_filename = f"{video_id}_{file.filename}"
    raw_path = os.path.join(RAW_DIR, raw_filename)
    file.save(raw_path)

    add_log("GATEWAY", f"Menerima upload '{file.filename}' -> Disimpan sebagai {raw_filename}")

    try:
        add_log("SEGMENTER", f"Memecah video {video_id} menjadi chunks durasi 5s...")
        chunks = split_video_into_chunks(raw_path, video_id, chunk_duration=5)
        add_log("SEGMENTER", f"Berhasil memecah menjadi {len(chunks)} potongan video.")
    except Exception as error:
        add_log("SEGMENTER", f"Gagal segmentasi: {error}", "error")
        return jsonify({"success": False, "error": f"Segmenter gagal: {error}"}), 500

    chunk_list = [
        {
            "index": i,
            "filename": os.path.basename(c),
            "status": "WAITING",
            "worker": "Menunggu Node...",
        }
        for i, c in enumerate(chunks)
    ]

    JOBS[video_id] = {
        "video_id": video_id,
        "status": "PROCESSING",
        "progress": 0,
        "resolution": resolution,
        "chunks_count": len(chunks),
        "raw_path": raw_path,
        "chunks": chunk_list,
    }

    thread = threading.Thread(target=background_transcode, args=(video_id, chunks, resolution))
    thread.daemon = True
    thread.start()

    return jsonify({
        "success": True,
        "video_id": video_id,
        "chunks_count": len(chunks),
        "resolution": resolution,
    })


@app.route("/api/v1/videos/<video_id>", methods=["GET"])
def get_video_status(video_id: str):
    if video_id not in JOBS:
        return jsonify({"error": "Video ID not found"}), 404
    return jsonify(JOBS[video_id])


@app.route("/api/v1/videos/<video_id>/stream", methods=["GET"])
def stream_video(video_id: str):
    if video_id not in JOBS or JOBS[video_id].get("status") != "COMPLETED":
        return jsonify({"error": "Video not ready"}), 404

    output_path = JOBS[video_id]["output_file"]
    return send_file(output_path, mimetype="video/mp4")


@app.route("/api/v1/videos/<video_id>", methods=["DELETE"])
def delete_video_job(video_id: str):
    if video_id not in JOBS:
        return jsonify({"error": "Video ID not found"}), 404

    chunk_dir = os.path.join(ROOT_DIR, "storage", "chunks", video_id)
    if os.path.exists(chunk_dir):
        shutil.rmtree(chunk_dir, ignore_errors=True)

    del JOBS[video_id]
    add_log("MASTER", f"Job {video_id} cancelled and temporary chunks deleted.", "warn")
    return jsonify({"success": True, "message": f"Job {video_id} successfully deleted."}), 200


if __name__ == "__main__":
    lan_ip = get_local_ip()
    print("=" * 65)
    print("  DistriStream Master Node (API Gateway & Scheduler)")
    print(f"  Local Dashboard Web : http://localhost:{MASTER_REST_PORT}")
    print(f"  Network / 3-Laptop  : http://{lan_ip}:{MASTER_REST_PORT}")
    print("=" * 65)
    app.run(host="0.0.0.0", port=MASTER_REST_PORT, debug=False)
