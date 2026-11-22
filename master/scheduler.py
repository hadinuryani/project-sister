import os
import sys
import time
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

        # 1. Registered remote workers via dynamic push heartbeats (instant response, zero timeout lag)
        for url, info in list(self.worker_heartbeats.items()):
            if now - info["last_seen"] <= 15:
                worker_data = {
                    "url": info["url"],
                    "worker_id": info["worker_id"],
                    "instance_id": info["instance_id"],
                    "attributes": info["attributes"],
                }
                seen_instances[info["instance_id"]] = len(active_workers)
                active_workers.append(worker_data)

        # 2. Local fallback workers (127.0.0.1:5001, 5002) for single-node tests
        for url in self.worker_urls:
            try:
                response = requests.get(f"{url}/health", timeout=0.8)
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
        master_ip = get_local_ip()
        master_url = f"http://{master_ip}:{MASTER_REST_PORT}"

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
        }

        max_retries = 2
        for attempt in range(max_retries):
            try:
                response = requests.post(f"{worker_url}/rpc/transcode", json=payload, timeout=60)
                if response.status_code == 200:
                    return response.json()
            except Exception as error:
                print(f"[Master Client Stub] Retry {attempt + 1}/{max_retries} to {worker_url}: {error}")
                time.sleep(1)

        return {"success": False, "error": f"Worker {worker_url} unreachable after retry"}

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

        tasks = []
        for index, chunk_path in enumerate(chunks):
            assigned_worker = active_workers[index % len(active_workers)]
            chunk_filename = os.path.basename(chunk_path)
            output_filename = f"chunk_{index:03d}_{target_resolution}.mp4"
            output_chunk_path = os.path.join(CHUNKS_DIR, video_id, output_filename)
            task_id = f"{video_id}_chunk_{index}_{target_resolution}"

            if chunk_callback:
                chunk_callback(video_id, index, "DISPATCHED", assigned_worker["worker_id"])

            tasks.append({
                "index": index,
                "worker_url": assigned_worker["url"],
                "worker_id": assigned_worker["worker_id"],
                "task_id": task_id,
                "video_id": video_id,
                "chunk_filename": chunk_filename,
                "output_filename": output_filename,
                "input_path": chunk_path,
                "output_path": output_chunk_path,
                "resolution": target_resolution,
            })

        completed_count = 0
        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            future_to_task = {
                executor.submit(
                    self._call_worker_rpc_stub,
                    task["worker_url"],
                    task["task_id"],
                    1,
                    task["input_path"],
                    task["output_path"],
                    task["resolution"],
                    task["video_id"],
                    task["chunk_filename"],
                    task["output_filename"],
                ): task
                for task in tasks
            }

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                index = task["index"]

                try:
                    result = future.result()
                    if result.get("success"):
                        transcoded_chunks[index] = task["output_path"]
                        completed_count += 1
                        finished_worker = result.get("worker_id", task["worker_id"])
                        if chunk_callback:
                            chunk_callback(video_id, index, "DONE", finished_worker)
                        print(f"[Master] [Gather] Chunk {index + 1}/{len(chunks)} done via {finished_worker}")
                    else:
                        print(f"[Master] Chunk {index} failed on {task['worker_id']}. Attempting failover...")
                        fallback_workers = [w for w in active_workers if w["url"] != task["worker_url"]]
                        if fallback_workers:
                            fallback = fallback_workers[0]
                            if chunk_callback:
                                chunk_callback(video_id, index, "FAILOVER", fallback["worker_id"])
                            print(f"[Master] [Failover] Reassigning chunk {index} to {fallback['worker_id']}...")
                            retry_result = self._call_worker_rpc_stub(
                                fallback["url"],
                                task["task_id"],
                                2,
                                task["input_path"],
                                task["output_path"],
                                task["resolution"],
                                task["video_id"],
                                task["chunk_filename"],
                                task["output_filename"],
                            )
                            if retry_result.get("success"):
                                transcoded_chunks[index] = task["output_path"]
                                completed_count += 1
                                if chunk_callback:
                                    chunk_callback(video_id, index, "DONE", fallback["worker_id"])
                                print(f"[Master] [Failover Success] Chunk {index + 1} completed via {retry_result.get('worker_id')}")
                            else:
                                print(f"[Master] Failover for chunk {index} failed.")
                except Exception as error:
                    print(f"[Master] Exception on chunk {index}: {error}")

                if progress_callback:
                    percentage = int((completed_count / len(chunks)) * 100)
                    progress_callback(video_id, percentage, completed_count, len(chunks))

        if any(chunk is None for chunk in transcoded_chunks):
            raise RuntimeError("One or more chunks failed to transcode. Unable to assemble video.")

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
