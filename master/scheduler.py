import os
import queue
import socket
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

import requests

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common.config import CHUNKS_DIR, DEFAULT_WORKER_URLS, MASTER_REST_PORT, get_local_ip
from master.segmenter import merge_chunks


class MasterScheduler:
    def __init__(self, worker_urls: Optional[List[str]] = None):
        self.worker_urls = list(worker_urls or DEFAULT_WORKER_URLS)
        self.registered_workers: List[str] = []
        self.worker_heartbeats: Dict[str, Dict[str, Any]] = {}

    def register_worker_url(self, url: str) -> None:
        if url not in self.registered_workers and url not in self.worker_urls:
            self.registered_workers.append(url)

    def register_worker_heartbeat(
        self,
        url: str,
        worker_id: str,
        instance_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.worker_heartbeats[url] = {
            "url": url,
            "worker_id": worker_id or "Worker-Remote",
            "instance_id": instance_id or url,
            "attributes": attributes or {"codec": "H.264/AAC", "gpu_accel": False, "max_concurrent": 2},
            "last_seen": time.time(),
        }
        if url not in self.registered_workers and url not in self.worker_urls:
            self.registered_workers.append(url)

    def discover_workers(self) -> List[Dict[str, Any]]:
        active_workers: List[Dict[str, Any]] = []
        seen_instances: Dict[str, int] = {}
        now = time.time()

        # 1. Registered remote workers via dynamic push heartbeats (toleransi 30 detik agar tahan jitter Wi-Fi)
        for url, info in list(self.worker_heartbeats.items()):
            if now - info["last_seen"] <= 30:
                worker_data = {
                    "url": info["url"],
                    "worker_id": info["worker_id"],
                    "instance_id": info["instance_id"],
                    "attributes": info["attributes"],
                }
                seen_instances[info["instance_id"]] = len(active_workers)
                active_workers.append(worker_data)

        # 2. Local fallback workers (127.0.0.1:5001, 5002) hanya jika tidak ada worker remote terdaftar
        if not active_workers:
            for url in self.worker_urls:
                try:
                    response = requests.get(f"{url}/health", timeout=0.5)
                    if response.status_code == 200:
                        payload = response.json()
                        instance_id = payload.get("instance_id") or url
                        worker_data = {
                            "url": url,
                            "worker_id": payload.get("worker_id"),
                            "instance_id": instance_id,
                            "attributes": payload.get("attributes", {}),
                        }
                        if instance_id not in seen_instances:
                            seen_instances[instance_id] = len(active_workers)
                            active_workers.append(worker_data)
                except Exception:
                    continue

        return active_workers

    def _resolve_master_url_for_worker(self, worker_url: str) -> str:
        try:
            parsed = urllib.parse.urlparse(worker_url)
            worker_ip = parsed.hostname
            if worker_ip and worker_ip not in ("127.0.0.1", "localhost", "::1"):
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.connect((worker_ip, parsed.port or 80))
                    my_ip = sock.getsockname()[0]
                    return f"http://{my_ip}:{MASTER_REST_PORT}"
        except Exception:
            pass
        return f"http://{get_local_ip()}:{MASTER_REST_PORT}"

    def _call_worker_rpc_stub(
        self,
        worker_url: str,
        task_id: str,
        seq_num: int,
        input_path: str,
        output_path: str,
        resolution: str,
        video_id: str = "",
        chunk_filename: str = "",
        output_filename: str = "",
    ) -> Dict[str, Any]:
        master_url = self._resolve_master_url_for_worker(worker_url)
        parsed_worker = urllib.parse.urlparse(worker_url)
        worker_ip = parsed_worker.hostname or ""
        my_lan_ip = get_local_ip()
        is_remote = worker_ip not in ("127.0.0.1", "localhost", "::1", my_lan_ip)

        payload = {
            "task_id": task_id,
            "seq_num": seq_num,
            "input_path": input_path,
            "output_path": output_path,
            "target_resolution": resolution,
            "video_id": video_id,
            "chunk_filename": chunk_filename,
            "output_filename": output_filename,
            "master_url": master_url,
            "is_remote": is_remote,
        }

        max_retries = 2
        last_error = "Unknown error"
        for attempt in range(max_retries):
            try:
                response = requests.post(f"{worker_url}/rpc/transcode", json=payload, timeout=120)
                try:
                    res_json = response.json()
                except Exception:
                    res_json = None

                if response.status_code == 200 and res_json:
                    return res_json
                elif res_json and "error" in res_json:
                    last_error = res_json["error"]
                    print(f"[Master Client Stub] Worker {worker_url} reported error: {last_error}")
                    return {"success": False, "error": last_error}
                else:
                    last_error = f"Worker HTTP {response.status_code}"
            except Exception as error:
                last_error = str(error)
                print(f"[Master Client Stub] Retry {attempt + 1}/{max_retries} to {worker_url}: {error}")
                time.sleep(1)

        return {"success": False, "error": f"Worker {worker_url} gagal: {last_error}"}

    def dispatch_parallel_transcode(
        self,
        video_id: str,
        chunks: List[str],
        target_resolution: str = "720p",
        progress_callback: Optional[Callable[[str, int, int, int], None]] = None,
        chunk_callback: Optional[Callable[[str, int, str, str], None]] = None,
    ) -> Dict[str, Any]:
        active_workers = self.discover_workers()
        if not active_workers:
            raise RuntimeError("No active worker nodes found. Please start at least one worker.")

        print(f"\n[Master] Starting parallel transcoding for video: {video_id}")
        print(f"[Master]   Total chunks : {len(chunks)}")
        print(f"[Master]   Active nodes : {len(active_workers)} ({', '.join(w['worker_id'] for w in active_workers)})")
        print(f"[Master]   Resolution   : {target_resolution}")

        start_time = time.time()
        transcoded_chunks: List[Optional[str]] = [None] * len(chunks)

        # Antrean tugas per worker agar pembagian merata (Round-Robin Fair Distribution)
        worker_queues: Dict[str, queue.Queue] = {w["url"]: queue.Queue() for w in active_workers}

        for index, chunk_path in enumerate(chunks):
            chunk_filename = os.path.basename(chunk_path)
            output_filename = f"chunk_{index:03d}_{target_resolution}.mp4"
            output_chunk_path = os.path.join(CHUNKS_DIR, video_id, output_filename)
            task_id = f"{video_id}_chunk_{index}_{target_resolution}"
            task_info = {
                "index": index,
                "chunk_path": chunk_path,
                "chunk_filename": chunk_filename,
                "output_filename": output_filename,
                "output_chunk_path": output_chunk_path,
                "task_id": task_id,
            }
            # Distribusikan secara bergantian ke masing-masing laptop worker (Round-Robin)
            assigned_worker = active_workers[index % len(active_workers)]
            worker_queues[assigned_worker["url"]].put(task_info)

        completed_count = 0
        last_failure_reason = "No details"
        lock = threading.Lock()

        def worker_consumer(worker: Dict[str, Any]):
            nonlocal completed_count, last_failure_reason
            worker_id = worker["worker_id"]
            worker_url = worker["url"]

            while True:
                task = None
                # 1. Ambil dari antrean jatah worker ini sendiri
                try:
                    task = worker_queues[worker_url].get_nowait()
                except queue.Empty:
                    # 2. Jika jatah sendiri habis, bantu ambil dari antrean worker lain (Work Stealing)
                    with lock:
                        for other_url, other_q in worker_queues.items():
                            if other_url != worker_url and not other_q.empty():
                                try:
                                    task = other_q.get_nowait()
                                    break
                                except queue.Empty:
                                    pass
                    if not task:
                        break

                index = task["index"]
                chunk_path = task["chunk_path"]
                chunk_filename = task["chunk_filename"]
                output_filename = task["output_filename"]
                output_chunk_path = task["output_chunk_path"]
                task_id = task["task_id"]

                if chunk_callback:
                    chunk_callback(video_id, index, "DISPATCHED", worker_id)

                result = self._call_worker_rpc_stub(
                    worker_url=worker_url,
                    task_id=task_id,
                    seq_num=1,
                    input_path=chunk_path,
                    output_path=output_chunk_path,
                    resolution=target_resolution,
                    video_id=video_id,
                    chunk_filename=chunk_filename,
                    output_filename=output_filename,
                )

                success = bool(result.get("success") and os.path.exists(output_chunk_path))
                finished_worker = result.get("worker_id", worker_id)

                if not success:
                    worker_err = result.get("error") if not result.get("success") else "Chunk file not found on Master"
                    print(f"[Master] Chunk {index} failed on {worker_id} ({worker_err}). Attempting failover...")

                    fallback_workers = [w for w in active_workers if w["url"] != worker_url]
                    if fallback_workers:
                        fallback = fallback_workers[index % len(fallback_workers)]
                        if chunk_callback:
                            chunk_callback(video_id, index, "FAILOVER", fallback["worker_id"])
                        print(f"[Master] [Failover] Reassigning chunk {index} to {fallback['worker_id']}...")
                        retry_result = self._call_worker_rpc_stub(
                            worker_url=fallback["url"],
                            task_id=task_id,
                            seq_num=2,
                            input_path=chunk_path,
                            output_path=output_chunk_path,
                            resolution=target_resolution,
                            video_id=video_id,
                            chunk_filename=chunk_filename,
                            output_filename=output_filename,
                        )
                        if retry_result.get("success") and os.path.exists(output_chunk_path):
                            success = True
                            finished_worker = retry_result.get("worker_id", fallback["worker_id"])
                            print(f"[Master] [Failover Success] Chunk {index + 1} completed via {finished_worker}")
                        else:
                            last_failure_reason = retry_result.get("error", "Failover failed")
                    else:
                        last_failure_reason = worker_err

                with lock:
                    if success:
                        transcoded_chunks[index] = output_chunk_path
                        completed_count += 1
                        if chunk_callback:
                            chunk_callback(video_id, index, "DONE", finished_worker)
                        print(f"[Master] [Gather] Chunk {index + 1}/{len(chunks)} done via {finished_worker}")
                    else:
                        print(f"[Master] ❌ Chunk {index + 1} gagal diproses.")

                    if progress_callback:
                        percentage = int((completed_count / len(chunks)) * 100)
                        progress_callback(video_id, percentage, completed_count, len(chunks))

        # Jalankan worker consumer threads terkelola (Dynamic Load Balancing)
        threads = []
        for worker in active_workers:
            slots = worker.get("attributes", {}).get("max_concurrent", 2)
            slots = max(1, min(slots, 3))
            for _ in range(slots):
                t = threading.Thread(target=worker_consumer, args=(worker,))
                t.daemon = True
                threads.append(t)
                t.start()

        for t in threads:
            t.join()

        if any(chunk is None for chunk in transcoded_chunks):
            raise RuntimeError(f"Gagal memproses video: Sebagian chunk gagal ditranscode. Detail: {last_failure_reason}")

        total_time_ms = int((time.time() - start_time) * 1000)
        final_video_path = merge_chunks(video_id, target_resolution, [c for c in transcoded_chunks if c])

        print(f"\n[Master] Assembled final video: {final_video_path}")
        print(f"[Master] Total parallel execution time: {total_time_ms / 1000:.2f} seconds\n")

        return {
            "video_id": video_id,
            "resolution": target_resolution,
            "output_file": final_video_path,
            "chunks_count": len(chunks),
            "workers_used": len(active_workers),
            "total_time_ms": total_time_ms,
        }
