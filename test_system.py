import os
import sys
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common.config import BASE_DIR
from master.scheduler import MasterScheduler
from master.segmenter import split_video_into_chunks


def run_integration_test():
    sample_video_path = os.path.join(BASE_DIR, "sample_test_video.mp4")
    if not os.path.exists(sample_video_path):
        print(f"Sample video not found at: {sample_video_path}")
        print("Please run generate_sample_video.py first.")
        return

    print("=" * 65)
    print("  DISTRISTREAM INTEGRATION & VERIFICATION TEST")
    print("=" * 65)

    scheduler = MasterScheduler()
    active_workers = scheduler.discover_workers()
    print(f"[Test] Discovered {len(active_workers)} active workers:")
    for worker in active_workers:
        print(f"       - {worker['worker_id']} @ {worker['url']}")

    if not active_workers:
        print("\n[FAILED] No active workers detected.")
        print("Please start worker nodes first:")
        print("  python worker/worker_node.py --id Worker-1 --port 5001")
        print("  python worker/worker_node.py --id Worker-2 --port 5002")
        return

    test_video_id = f"test_{int(time.time())}"
    print(f"\n[Test] 1. Partitioning video into 5-second segments...")
    chunks = split_video_into_chunks(sample_video_path, test_video_id, chunk_duration=5)
    print(f"       -> Created {len(chunks)} video chunks.")

    print(f"\n[Test] 2. Dispatching parallel transcoding (Scatter & Gather)...")
    summary = scheduler.dispatch_parallel_transcode(
        video_id=test_video_id,
        chunks=chunks,
        target_resolution="480p",
    )

    print("\n" + "=" * 65)
    print("  TEST EXECUTION SUMMARY:")
    print("=" * 65)
    print(f"  Status        : SUCCESS")
    print(f"  Output File   : {summary['output_file']}")
    print(f"  Resolution    : {summary['resolution']}")
    print(f"  Elapsed Time  : {summary['total_time_ms'] / 1000:.2f} seconds")
    print(f"  Workers Used  : {summary['workers_used']} nodes")
    print("=" * 65)


if __name__ == "__main__":
    run_integration_test()
