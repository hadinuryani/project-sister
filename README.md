# Panduan Menjalankan Program DistriStream Engine v1.0
### Platform Transcoding Video Terdistribusi & Rendering Paralel

Aplikasi ini mengimplementasikan spesifikasi resmi **Product Requirement Document (PRD) DistriStream Engine v1.0** untuk mata kuliah Sistem Terdistribusi.

---

## 🏗️ Struktur Proyek

```text
distristream/
│
├── common/                  # Konfigurasi bersama seluruh node
│   └── config.py            # Path storage, port, dan deteksi FFmpeg otomatis
│
├── dokumentasi/             # Dokumen Spesifikasi Resmi PRD Tugas Akhir
│   ├── PRD-DistriStream-Engine-v1.0.pdf   # Dokumen PRD siap kumpul & presentasi (PDF)
│   ├── PRD-DistriStream-Engine-v1.0.docx  # Dokumen PRD editable Microsoft Word
│   ├── PRD-DistriStream-Engine-v1.0.md    # Naskah PRD format Markdown
│   └── prd-distrisearch-analytics-v2.pdf  # Berkas acuan contoh dari dosen
│
├── master/                  # Node Master (Koordinator & API Gateway)
│   ├── app.py               # REST API Server & Web Dashboard Interaktif
│   ├── scheduler.py         # Orkestrator Scatter-Gather & RPC Client Stub
│   └── segmenter.py         # Pemotong video (chunking) dan penggabung hasil
│
├── worker/                  # Node Worker (Kuli Transcoding)
│   ├── worker_node.py       # RPC Server Stub & penanganan At-most-once
│   └── transcoder.py        # Eksekusi FFmpeg konversi resolusi
│
├── storage/                 # Repositori Penyimpanan File
│   ├── raw_uploads/         # Video mentah asli yang diunggah
│   ├── chunks/              # Potongan durasi video (5 detik/chunk)
│   └── output_hls/          # Video utuh hasil transcoding
│
├── generate_sample_video.py # Generator video uji coba otomatis (15 detik)
├── run_cluster.py           # Peluncur seluruh kluster (Master + 2 Worker) otomatis
└── test_system.py           # Script pengujian integrasi otomatis di terminal
```

---

## 🚀 Cara Menjalankan Program (Pilih Salah Satu Cara)

### Cara 1: Menjalankan Seluruh Kluster Sekaligus (Paling Mudah)
Cukup buka terminal dan jalankan:
```bash
python run_cluster.py
```
Script ini akan otomatis menyalakan:
1. **Worker Node 1** pada port `http://localhost:5001`
2. **Worker Node 2** pada port `http://localhost:5002`
3. **Master Node** pada port `http://localhost:5000`

Buka browser kamu di: **[http://localhost:5000](http://localhost:5000)** untuk melihat dashboard web!

---

### Cara 2: Menjalankan Manual di 3 Terminal Terpisah (1 Laptop)

Jika ingin memperlihatkan ke dosen di 1 laptop bahwa masing-masing node berjalan pada proses dan port terpisah:

* **Terminal 1 (Worker 1):**
  ```bash
  python worker/worker_node.py --id Worker-1 --port 5001
  ```
* **Terminal 2 (Worker 2):**
  ```bash
  python worker/worker_node.py --id Worker-2 --port 5002
  ```
* **Terminal 3 (Master Node):**
  ```bash
  python master/app.py
  ```
Lalu buka browser di: **[http://localhost:5000](http://localhost:5000)**.

---

### Cara 3: Menjalankan Nyata di 3 Laptop Fisik Berbeda via Wi-Fi / Hotspot (SANGAT DIREKOMENDASIKAN UNTUK DEMO KELAS ⭐⭐⭐)

Ini adalah cara paling memukau saat presentasi karena membuktikan sistem benar-benar terdistribusi secara fisik antar-perangkat.

1. **Sambungkan ketiga laptop ke Wi-Fi / Hotspot HP yang sama.**
2. **Laptop 1 (Master / Server Utama):**
   Jalankan di terminal:
   ```bash
   python master/app.py
   ```
   Amati terminal Laptop 1, akan muncul alamat IP jaringannya, contoh:  
   `Network / 3-Laptop : http://192.168.1.15:5000`
3. **Laptop 2 (Worker 1 - Teman Kelompok A):**
   Jalankan di terminal:
   ```bash
   python worker/worker_node.py --id Laptop-Budi --port 5001 --master http://192.168.1.15:5000
   ```
   *(Ganti `192.168.1.15` dengan IP Laptop 1 yang muncul di terminal).*
4. **Laptop 3 (Worker 2 - Teman Kelompok B):**
   Jalankan di terminal:
   ```bash
   python worker/worker_node.py --id Laptop-Siti --port 5001 --master http://192.168.1.15:5000
   ```
5. **Buka Web Dashboard di Laptop 1 (`http://localhost:5000`):**
   * Di layar dashboard, kartu **Laptop-Budi** dan **Laptop-Siti** akan otomatis muncul berstatus **ONLINE (Hijau)**.
   * Pilih video dan klik **Mulai Transcoding Paralel**.
   * Perhatikan terminal di Laptop 2 dan Laptop 3: masing-masing akan mengunduh potongan video lewat Wi-Fi, memproses transcoding menggunakan CPU masing-masing secara simultan, lalu mengirimkan balik hasilnya ke Laptop 1 untuk digabungkan secara otomatis!

---

## 🎯 Cara Melakukan Pengujian / Demo

1. Buka dashboard web di `http://localhost:5000`.
2. Kamu akan melihat status **Worker-1** dan **Worker-2** berstatus **Online (Hijau)**.
3. Pilih file video (kamu bisa menggunakan file `sample_test_video.mp4` yang sudah disediakan di folder).
4. Pilih resolusi target (misal `720p HD` atau `480p SD`).
5. Klik tombol **"Mulai Transcoding Paralel"**.
6. Amati terminal:
   * Master akan memotong video menjadi beberapa potongan (misal 3 potongan).
   * Potongan 1 dikirim ke Worker-1.
   * Potongan 2 dikirim ke Worker-2.
   * Kedua worker memproses secara bersamaan (*parallel transcoding*).
   * Master mengumpulkan kembali hasil potongan dan menyatukannya menjadi video utuh.
7. Video langsung bisa diputar di pemutar video browser!
