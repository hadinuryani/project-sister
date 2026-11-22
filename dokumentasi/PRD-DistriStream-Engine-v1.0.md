# Product Requirement Document (PRD)
## DistriStream Engine v1.0
### Platform Transcoding Video Paralel & Rendering Terdistribusi Skala Besar

| Atribut Dokumen | Keterangan Spesifikasi |
| :--- | :--- |
| **Mata Kuliah** | Sistem Terdistribusi 2026 PSTIF UTM |
| **Nama Produk** | DistriStream Engine |
| **Versi Dokumen** | 1.0 (Spesifikasi Implementasi RPC, REST API & Scatter-Gather Engine) |
| **Tanggal Spesifikasi** | Oktober 2026 |
| **Arsitektur Utama** | Hibrida Multitier Master-Worker (REST API, RPC Middleware, Scatter-Gather Engine, Distributed Storage) |
| **Target Pengguna** | Video on Demand (VOD) Platforms, Distributed Media Processing Systems, OTT Streaming Providers |

---

## Daftar Isi
1. [Ringkasan Eksekutif & Visi Produk](#1-ringkasan-eksekutif--visi-produk)
2. [Arsitektur Sistem & Pola Desain Software](#2-arsitektur-sistem--pola-desain-software)
   - 2.1 [Pembagian Layer & Tiering](#21-pembagian-layer--tiering)
   - 2.2 [Topologi Kluster & Alur Data](#22-topologi-kluster--alur-data)
3. [Skema Penamaan (Naming System)](#3-skema-penamaan-naming-system)
   - 3.1 [Penamaan Berstruktur (Structured Naming)](#31-penamaan-berstruktur-structured-naming)
   - 3.2 [Penamaan Flat Terdistribusi (Flat Naming)](#32-penamaan-flat-terdistribusi-flat-naming)
   - 3.3 [Penamaan Berbasis Atribut (Attribute-Based Naming)](#33-penamaan-berbasis-atribut-attribute-based-naming)
4. [Spesifikasi Antarmuka RESTful API](#4-spesifikasi-antarmuka-restful-api)
5. [Spesifikasi Implementasi Remote Procedure Call (RPC)](#5-spesifikasi-implementasi-remote-procedure-call-rpc)
   - 5.1 [Komponen & Alur Eksekusi RPC Stub (10 Tahapan)](#51-komponen--alur-eksekusi-rpc-stub-10-tahapan)
   - 5.2 [Kebijakan Pengiriman Parameter (Parameter Passing)](#52-kebijakan-pengiriman-parameter-parameter-passing)
   - 5.3 [Standardisasi Representasi Data (Data Representation)](#53-standardisasi-representasi-data-data-representation)
   - 5.4 [Penanganan Kegagalan & Semantik Eksekusi RPC (Call Semantics)](#54-penanganan-kegagalan--semantik-eksekusi-rpc-call-semantics)
   - 5.5 [Transport Layer & Prinsip End-to-End Arguments](#55-transport-layer--prinsip-end-to-end-arguments)
6. [Lapisan Komputasi Paralel Berbasis Konsep MPI & MapReduce](#6-lapisan-komputasi-paralel-berbasis-konsep-mpi--mapreduce)
   - 6.1 [Pemetaan Operasi Kolektif MPI (Scatter & Gather)](#61-pemetaan-operasi-kolektif-mpi-scatter--gather)
   - 6.2 [Komunikasi Asinkron & Non-Blocking I/O](#62-komunikasi-asinkron--non-blocking-io)
   - 6.3 [Model Kinerja Paralel & Analisis Amdahl's Law](#63-model-kinerja-paralel--analisis-amdahls-law)
7. [Strategi Caching & Konsistensi Data](#7-strategi-caching--konsistensi-data)
   - 7.1 [Hierarki & Lokasi Caching](#71-hierarki--lokasi-caching)
   - 7.2 [Kebijakan Penggantian Cache (LRU Replacement Policy)](#72-kebijakan-penggantian-cache-lru-replacement-policy)
   - 7.3 [Kebijakan Konsistensi Data & Lease](#73-kebijakan-konsistensi-data--lease)
8. [Matriks Persyaratan Fungsional & Non-Fungsional](#8-matriks-persyaratan-fungsional--non-fungsional)

---

## 1. Ringkasan Eksekutif & Visi Produk
**DistriStream Engine v1.0** merupakan platform transcoding video paralel terdistribusi berskala besar yang dirancang untuk menjawab tantangan pemrosesan berkas multimedia beresolusi tinggi. Proses konversi video (*video transcoding*) tradisional yang berjalan pada node tunggal (*monolithic/vertical scaling*) kerap mengalami degradasi performa drastis akibat saturasi CPU, keterbatasan ruang memori (*processor-memory speed gap*), dan tingginya latensi kompresi codec video seperti H.264/AVC.

DistriStream Engine mengatasi permasalahan tersebut melalui penerapan **skalabilitas horizontal** menggunakan pendekatan **Scatter-Gather Parallel Computing** dan arsitektur terdistribusi modern. Berkas video masukan dipartisi secara cerdas menjadi serangkaian segmen temporal berdurasi seragam (*video chunks*). Segmen-segmen ini kemudian didistribusikan secara simultan ke kluster *Worker Nodes* yang independen melalui protokol **Remote Procedure Call (RPC)** bersemantik *At-most-once*. Setelah komputasi paralel selesai, Master Node mengagregasi kembali hasil parsial menjadi berkas video final yang koheren tanpa degradasi sinkronisasi audio-visual.

Sistem ini memadukan **6 pilar utama sistem terdistribusi modern** yang selaras dengan kurikulum perkuliahan Sistem Terdistribusi:
1. **Arsitektur Sistem Terdistribusi (Multitier Master-Worker):** Isolasi tanggung jawab antara antarmuka pengguna, API gateway/orkestrator, pemroses data (worker), dan repositori penyimpanan berkas.
2. **Sistem Penamaan Komprehensif (Structured, Flat, & Attribute-Based Naming):** Identifikasi hierarkis endpoint API, pengalamatan unik segmen video, dan discovery kapabilitas perangkat keras pekerja.
3. **Antarmuka Layanan RESTful API:** Titik masuk terstandar, stateless, dan aman untuk konsumsi data pengguna dan integrasi sistem luar.
4. **Remote Procedure Call (RPC) & Call Semantics:** Komunikasi antar-proses internal dengan isolasi ruang alamat (*address space separation*), marshaling/unmarshaling, serta penanganan kegagalan bergaransi *At-most-once*.
5. **Komputasi Paralel Pola MPI (Message Passing Interface):** Emulasi operasi kolektif *Scatter* dan *Gather* dengan eksekusi *non-blocking* / *asynchronous thread pool*.
6. **Strategi Caching & Konsistensi Data:** Pemanfaatan *LRU Cache* dan tabel idempotensi untuk eliminasi duplikasi komputasi dan optimasi latensi respon.

---

## 2. Arsitektur Sistem & Pola Desain Software

### 2.1 Pembagian Layer & Tiering
Sistem DistriStream Engine menerapkan arsitektur bertingkat (**4-Tier Multitiered Architecture**) untuk menjamin modularitas, skalabilitas, dan toleransi isolasi kesalahan (*fault isolation*):

* **Tier 1 - Presentation & Telemetry Layer:** Antarmuka web interaktif berbasis HTML5 dan CSS kustom responsif real-time. Menyediakan *drag-and-drop video uploader*, pemilih profil resolusi (720p HD, 480p SD, 360p Mobile), pelacak progres persentase transcode, log sistem langsung (*live cluster logs*), dan *HTML5 Video Player* bawaan.
* **Tier 2 - API Gateway & Orchestrator Layer (Master Node):** Berfungsi sebagai *single entry point* yang menerima permintaan klien via HTTP RESTful API. Layer ini mengelola antrean pekerjaan (*job queue*), memotong video menjadi segmen (*segmenter*), menjadwalkan pembagian beban ke worker (*scheduler*), serta menggabungkan kembali segmen hasil render (*reassembler*).
* **Tier 3 - RPC Middleware & Parallel Worker Cluster:** Terdiri dari simpul pekerja (*Worker Nodes*) terdistribusi. Setiap worker menjalankan *Server Stub RPC* yang mendengarkan permintaan komputasi, mengonfigurasi *FFmpeg transcoding engine*, menerapkan penapisan duplikasi (*duplicate filtering*), dan mengembalikan metrik eksekusi ke Master.
* **Tier 4 - Storage & Chunk Repository Layer:** Repositori penyimpanan berkas yang terbagi menjadi direktori `raw_uploads/` (video mentah), `chunks/` (potongan segmen video dan skrip perakitan `concat.txt`), serta `output_hls/` (video hasil rendering final).

### 2.2 Topologi Kluster & Alur Data
Topologi sistem mengadopsi pola **Master-Worker (Master-Slave)**:
1. **Master Node (Port 5000):** Koordinator sentral yang mempertahankan tabel status kerja (`JOBS`), mencatat log sistem, dan mengelola orkestrasi kluster.
2. **Worker Node 1..N (Port 5001, 5002, ...):** Kuli komputasi otonom yang dapat ditambah atau dikurangi (*horizontal auto-scaling*) tanpa memerlukan restart pada Master Node.

---

## 3. Skema Penamaan (Naming System)

Sistem mengombinasikan 3 skema penamaan terdistribusi untuk memenuhi kebutuhan resolusi yang berbeda:

### 3.1 Penamaan Berstruktur (Structured Naming)
Digunakan pada antarmuka RESTful API eksternal dan rute RPC internal. Mengikuti struktur hierarki berkas (*naming graph*) berbasis URI:
* `/api/v1/videos/upload` : Endpoint penyerahan berkas video mentah.
* `/api/v1/videos/{video_id}` : Resolusi status objek pekerjaan berdasarkan pengenal unik.
* `/api/v1/videos/{video_id}/stream` : Resolusi berkas multimedia biner untuk konsumsi pemutar video.
* `/rpc/transcode` : Rute prosedur internal antarkomponen untuk pemanggilan fungsi komputasi.

### 3.2 Penamaan Flat Terdistribusi (Flat Naming)
Setiap entitas tugas dan segmen berkas diberi pengenal unik datar (*flat identifier*) berukuran tetap tanpa ketergantungan pada lokasi fisik simpul:
* **Video Identifier (`video_id`):** Dihasilkan menggunakan fungsi Pseudo-Random Unique Hexadecimal 8-karakter (misal: `1fe5dfd3`, `884310df`) untuk menghindari tabrakan nama (*name collision*).
* **Canonical Chunk Key:** Mengikuti format kanonikal:  
  `{video_id}/chunk_{seq_num:03d}_{resolution}.mp4`  
  Format ini berfungsi sebagai *content key* yang deterministik, memudahkan verifikasi integritas segmen secara terdistribusi.

### 3.3 Penamaan Berbasis Atribut (Attribute-Based Naming)
Digunakan oleh Master Scheduler untuk melakukan pencocokan pekerjaan (*workload matching*) terhadap kapabilitas perangkat keras simpul pekerja. Setiap worker mempublikasikan tupel pasangan `(attribute, value)` melalui endpoint *heartbeat*:
* `codec = "H.264/AAC"` (Mendukung enkoding video H.264 dan audio AAC)
* `gpu_accel = False / True` (Status ketersediaan akselerasi grafis perangkat keras)
* `max_concurrent = 2` (Kapasitas batas thread komputasi lokal yang aman)
* `worker_id = "Worker-1"` (Nama simbolik simpul dalam kluster)

---

## 4. Spesifikasi Antarmuka RESTful API

Antarmuka eksternal diimplementasikan berbasis arsitektur RESTful yang stateless dan decoupled:

| HTTP Method | Endpoint | Format Payload / Param | Fungsi & Perilaku Sistem |
| :--- | :--- | :--- | :--- |
| **POST** | `/api/v1/videos/upload` | `multipart/form-data` (`video_file`, `resolution`) | Menerima berkas video, menggenerasi `video_id`, memvalidasi format berkas, memotong video menjadi segmen 5 detik, dan memicu eksekusi paralel. |
| **GET** | `/api/v1/videos/{video_id}` | Path Parameter: `video_id` | Mengambil status pemrosesan (`QUEUED`, `PROCESSING`, `MERGING`, `COMPLETED`, `FAILED`), persentase progress, durasi eksekusi, dan nama berkas keluaran. |
| **GET** | `/api/v1/videos/{video_id}/stream` | Path Parameter: `video_id` | Menyajikan berkas biner video hasil komputasi (MIME-type `video/mp4`) dengan dukungan HTTP Range Requests (scrubbing/seeking). |
| **GET** | `/api/v1/workers` | None | Mengembalikan daftar seluruh *Worker Nodes* yang terdaftar, status konektivitas (`ONLINE`/`OFFLINE`), alamat URL, dan atribut kapabilitas hardware. |
| **GET** | `/api/v1/logs` | None | Mengambil rekaman log aktivitas kluster terdistribusi (maksimal 40 log kejadian terbaru) untuk inspeksi dan visualisasi dashboard. |
| **DELETE** | `/api/v1/videos/{video_id}` | Path Parameter: `video_id` | Membatalkan tugas transcoding yang sedang berjalan dan membersihkan seluruh berkas segmen temporer dari *Storage Layer*. |

---

## 5. Spesifikasi Implementasi Remote Procedure Call (RPC)

Lapisan komunikasi antar-layanan internal (Master-ke-Worker) diimplementasikan menggunakan arsitektur **Remote Procedure Call (RPC)** guna menyediakan transparansi akses (*access transparency*), sehingga Master Scheduler dapat memanggil prosedur komputasi di Worker Node layaknya fungsi lokal.

### 5.1 Komponen & Alur Eksekusi RPC Stub (10 Tahapan)
Alur pemanggilan fungsi `TranscodeChunk(input, output, resolution)` mengikuti 10 langkah baku sistem terdistribusi:
1. **Client Call:** Master Scheduler memanggil fungsi lokal pada *Client Stub* dengan parameter tugas pemrosesan.
2. **Parameter Marshalling:** *Client Stub* mengemas struktur data Python (kamus tugas, parameter resolusi, nomor sekuens, dan path berkas) menjadi format pesan jaringan standar (JSON/byte stream payload).
3. **Transmission:** Sistem Operasi Master mengirimkan paket pesan melalui antarmuka jaringan TCP ke simpul Worker tujuan.
4. **Delivery:** Sistem Operasi Worker menerima paket jaringan dan meneruskannya ke proses *Server Stub*.
5. **Unmarshalling:** *Server Stub* membongkar (*unpacking*) paket pesan menjadi variabel lokal dan memvalidasi keabsahan parameter.
6. **Local Call:** *Server Stub* melakukan pemanggilan fungsi lokal asli `transcode_video_chunk()` yang mengontrol pustaka FFmpeg.
7. **Execution:** Proses worker mengeksekusi kompresi dan perubahan skala video menggunakan CPU/GPU lokal, menghasilkan metrik waktu eksekusi (`duration_ms`).
8. **Result Marshalling:** *Server Stub* mengemas hasil keluaran (`success`, `duration_ms`, `output_path`, `task_id`) ke dalam pesan balasan.
9. **Return Transmission:** Sistem Operasi Worker mentransmisikan pesan balasan melalui soket TCP kembali ke Master Node.
10. **Result Unmarshalling & Return:** *Client Stub* membongkar pesan hasil balasan dan mengembalikannya ke Master Scheduler untuk diagregasi.

### 5.2 Kebijakan Pengiriman Parameter (Parameter Passing)
* **Call-by-Value:** Tipe data skalar seperti nomor sekuens (`seq_num`), target resolusi (`"720p"`), dan pengenal tugas dikirimkan langsung sebagai nilai literal di dalam payload.
* **Pass-by-Copying & Copy/Restore:** Karena ruang alamat memori Master dan Worker terisolasi secara fisik, pointer berkas tidak dapat dikirim secara langsung. Sistem menerapkan *Pass-by-Copying* pada deskriptor berkas dan metadata segmen, kemudian mengembalikan salinan status eksekusi terbaru (*Copy/Restore*) ke memori Master.

### 5.3 Standardisasi Representasi Data (Data Representation)
* **Byte Ordering (Endianness):** Semua data biner numerik diseragamkan dalam format Little-Endian sesuai standar arsitektur x86-64.
* **Format Pengodean:** Struktur data serialisasi menggunakan representasi teks UTF-8 terstandar, bilangan bulat bertanda menggunakan representasi Two's Complement, dan penanda durasi desimal mematuhi standar IEEE 754 Floating Point.

### 5.4 Penanganan Kegagalan & Semantik Eksekusi RPC (Call Semantics)
Sistem menerapkan semantik eksekusi **At-most-once** yang dilengkapi penapisan duplikasi (*duplicate filtering*):

| Karakteristik Semantik | At-least-once | At-most-once (Diterapkan DistriStream) | Exactly-once (Ideal) |
| :--- | :--- | :--- | :--- |
| **Retransmisi Pesan** | Ya (Klien kirim ulang jika timeout) | Ya (Master Stub kirim ulang maksimal 2x) | Ya (Dikelola protokol transaksional) |
| **Penapisan Duplikasi** | Tidak Ada | **Ya (Server melacak Sequence Numbers & Task ID)** | Ya (Berbasis State Mesin & Log Durabel) |
| **Perilaku Eksekusi** | Prosedur dapat dieksekusi berkali-kali. Berbahaya untuk transcoding (boros CPU). | **Prosedur dijamin dieksekusi maksimal 1 kali. Jika duplikat datang, server kirim hasil lama.** | Prosedur dieksekusi tepat satu kali dalam seluruh siklus hidup sistem. |

Worker Node memelihara tabel penapisan duplikasi `PROCESSED_REQUESTS`. Setiap permintaan RPC membawa kunci komposit `{task_id}_{seq_num}`. Jika sebuah permintaan dengan kunci yang sama diterima kembali (akibat timeout pada Master), Worker tidak menjalankan proses encoding FFmpeg ulang, melainkan langsung mengembalikan hasil eksekusi dari memori cache (*retransmit cached reply*).

### 5.5 Transport Layer & Prinsip End-to-End Arguments
Sistem menerapkan prinsip *End-to-End Arguments* (Saltzer et al.): meskipun protokol transport TCP menjamin pengiriman paket bebas kesalahan di tingkat soket, verifikasi keandalan sejati tetap berada di tingkat aplikasi. Master Scheduler memverifikasi eksistensi fisik berkas segmen keluaran, ukuran byte non-nol, serta kode status proses sebelum menyatakan suatu tahap *Gather* berhasil.

---

## 6. Lapisan Komputasi Paralel Berbasis Konsep MPI & MapReduce

### 6.1 Pemetaan Operasi Kolektif MPI (Scatter & Gather)
Pemrosesan multimedia pada DistriStream Engine memetakan pola komunikasi kolektif dari standar **Message Passing Interface (MPI)**:
* **Grup Komunikator (`COMM_WORLD` Analog):** Master bertindak sebagai koordinator (Rank 0), sedangkan Worker Node 1..N bertindak sebagai simpul komputasi (Rank 1..N).
* **Operasi MPI_Scatter:** Berkas video masukan yang berdurasi panjang dipotong menjadi potongan berukuran tetap ($T_{chunk} = 5$ detik). Master mendistribusikan potongan-potongan tersebut secara merata (*Round-Robin Partitioning*) ke antrean seluruh worker yang berstatus `ONLINE`.
* **Operasi MPI_Gather:** Setelah seluruh worker menyelesaikan konversi pada segmen masing-masing, Master mengumpulkan kembali seluruh potongan yang telah di-transcode, mengurutkan berdasarkan indeks urutan waktu asli, dan menggabungkannya secara deterministik (*FFmpeg demuxer concat*).

### 6.2 Komunikasi Asinkron & Non-Blocking I/O
Untuk menghindari deadlock dan menyembunyikan latensi transmisi jaringan, Master Scheduler menggunakan eksekutor multithreaded (`ThreadPoolExecutor`) yang mengemulasikan sifat non-blocking dari `MPI_Isend` dan `MPI_Irecv`. Master dapat menyiarkan tugas ke banyak worker secara simultan tanpa terhenti (*blocking*) menunggu satu worker menyelesaikan pekerjaannya.

### 6.3 Model Kinerja Paralel & Analisis Amdahl's Law
Peningkatan kecepatan komputasi (*speedup*) pada kluster pemrosesan video paralel dianalisis menggunakan Hukum Amdahl (*Amdahl's Law*):

$$S_p = \frac{T_1}{T_p} = \frac{1}{(1 - P) + \frac{P}{N}}$$

Keterangan variabel:
* $T_1$ : Waktu eksekusi video pada simpul tunggal (sekuensial murni).
* $T_p$ : Waktu eksekusi total pada sistem terdistribusi dengan $N$ worker.
* $P$ : Fraksi komputasi yang dapat diparalelkan secara independen (proses transcoding chunk video, $\approx 85\%$ dari total waktu komputasi).
* $(1 - P)$ : Fraksi komputasi sekuensial yang tidak dapat diparalelkan (proses pemotongan segmen awal dan penyambungan berkas akhir, $\approx 15\%$).
* $N$ : Jumlah simpul pekerja (*worker nodes*).

Dengan konfigurasi 2 Worker ($N = 2$), percepatan teoritis mencapai:
$$S_2 = \frac{1}{0.15 + \frac{0.85}{2}} = \frac{1}{0.15 + 0.425} = 1.74\times$$
Hasil pengujian empiris sistem mencatatkan waktu pemrosesan video sampel 15 detik turun dari $\approx 18$ detik (single worker) menjadi $\approx 11$ detik (dual worker), mendekati batas percepatan teoritis.

---

## 7. Strategi Caching & Konsistensi Data

### 7.1 Hierarki & Lokasi Caching
Sistem menerapkan strategi caching pada 3 tingkatan guna meminimalkan latensi dan beban I/O:
1. **Client / Browser Cache (Edge Layer):** Menggunakan header respons HTTP `Cache-Control: public, max-age=3600` untuk berkas video keluaran yang bersifat statis.
2. **Master Job Cache (Memory Layer):** Menyimpan metadata pekerjaan dan status telemetri di memori utama Master Node (`JOBS` dictionary) guna melayani kueri polling klien berkecepatan tinggi (< 10 ms).
3. **Worker Result Cache (Idempotency Layer):** Menyimpan respon eksekusi RPC pada `PROCESSED_REQUESTS` untuk mendukung penapisan duplikasi *At-most-once*.

### 7.2 Kebijakan Penggantian Cache (LRU Replacement Policy)
Repositori penyimpanan segmen menerapkan algoritma **Least Recently Used (LRU)**:
* Objek segmen yang paling lama tidak diakses oleh proses merge akan diprioritaskan untuk dibersihkan ketika batas kuota ruang disk tercapai.
* Kebijakan LRU memenuhi **Stack Property**, menjamin bahwa penambahan kapasitas penyimpanan tidak akan pernah menurunkan performa (*cache hit ratio*).

### 7.3 Kebijakan Konsistensi Data & Lease
* **Pemberian Hak Lease Temporal:** Berkas segmen mentah dan parsial di dalam `storage/chunks/{video_id}` diberi masa berlaku (*temporal lease*) selama 2 jam sejak proses merge selesai.
* **Pembersihan Berkala (*Invalidation Sweeper*):** Layanan pembersih di Master Node secara berkala mengeliminasi direktori kerja yang *lease*-nya telah kadaluwarsa guna mencegah kebocoran ruang penyimpanan (*disk exhaustion*).

---

## 8. Matriks Persyaratan Fungsional & Non-Fungsional

### 8.1 Persyaratan Fungsional (Functional Requirements)

| Kode ID | Kategori Modul | Prioritas | Deskripsi Kebutuhan Teknis Sistem |
| :--- | :--- | :--- | :--- |
| **FR-01** | REST API & Gateway | Tinggi (*Must Have*) | Sistem wajib menyediakan endpoint RESTful API terstandar untuk upload video, pengecekan status, streaming berkas, daftar worker, dan pembatalan tugas. |
| **FR-02** | RPC & Middleware | Tinggi (*Must Have*) | Sistem wajib mengimplementasikan komunikasi RPC antar-simpul dengan pemisahan *Client Stub* dan *Server Stub*, mendukung transmisi parameter *Pass-by-Copying*. |
| **FR-03** | Call Semantics | Tinggi (*Must Have*) | Layanan RPC wajib menerapkan semantik *At-most-once* dengan mekanisme penomoran sekuens dan *duplicate filtering cache* guna mencegah encoding ganda. |
| **FR-04** | Komputasi Paralel | Tinggi (*Must Have*) | Sistem wajib mengemulasikan pola *MPI Scatter-Gather* untuk membagi video menjadi potongan durasi 5 detik dan menyatukannya kembali secara otomatis. |
| **FR-05** | Service Discovery | Sedang (*Should Have*) | Master Node wajib mampu mendeteksi keberadaan dan kapabilitas simpul pekerja secara dinamis berbasis atribut (*Attribute-Based Discovery* via heartbeat). |
| **FR-06** | Pemantauan & Dashboard | Sedang (*Should Have*) | Sistem harus menyediakan antarmuka web interaktif yang menampilkan telemetri beban kerja kluster dan log aktivitas sistem secara langsung. |

### 8.2 Persyaratan Non-Fungsional (Non-Functional Requirements)

| Kode ID | Kategori Parameter | Nilai Tolok Ukur / Metrik Sasaran |
| :--- | :--- | :--- |
| **NFR-01** | **Kinerja & Percepatan Paralel** | Sistem harus menghasilkan *speedup* $S_p \ge 1.5\times$ saat dijalankan pada minimal 2 simpul pekerja aktif dibandingkan pemrosesan sekuensial. |
| **NFR-02** | **Toleransi Kesalahan (Fault Tolerance)** | Klien stub wajib melakukan percobaan ulang (*retry*) maksimal 2 kali pada kegagalan transmisi sementara tanpa menimbulkan inkonsistensi data. |
| **NFR-03** | **Skalabilitas Horizontal** | Kluster pekerja harus mendukung penambahan atau pengurangan node secara dinamis tanpa memerlukan *reboot* atau interupsi layanan pada Master Node. |
| **NFR-04** | **Transparansi Akses & Lokasi** | Klien tidak perlu mengetahui simpul pekerja mana yang memproses potongan video tertentu (*Location & Access Transparency*). |
