import os
import socket
import subprocess
import sys
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_EXE = sys.executable

PORTS_TO_CHECK = [5000, 5001, 5002]


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def main():
    print("=" * 65)
    print("  STARTING DISTRISTREAM DISTRIBUTED CLUSTER")
    print("=" * 65)
    print("  Components:")
    print("  1. Worker Node 1  -> http://localhost:5001")
    print("  2. Worker Node 2  -> http://localhost:5002")
    print("  3. Master Node    -> http://localhost:5000 (Web Dashboard)")
    print("=" * 65)

    occupied_ports = [port for port in PORTS_TO_CHECK if is_port_in_use(port)]
    if occupied_ports:
        print(f"\n[ERROR] Port(s) {occupied_ports} are already in use.")
        print("Please close any running Master/Worker instances and retry.")
        sys.exit(1)

    worker1_command = [
        PYTHON_EXE,
        os.path.join(ROOT_DIR, "worker", "worker_node.py"),
        "--id", "Worker-1",
        "--port", "5001",
    ]
    worker2_command = [
        PYTHON_EXE,
        os.path.join(ROOT_DIR, "worker", "worker_node.py"),
        "--id", "Worker-2",
        "--port", "5002",
    ]
    master_command = [
        PYTHON_EXE,
        os.path.join(ROOT_DIR, "master", "app.py"),
    ]

    print("\n[Launcher] Starting Worker Node 1...")
    worker1_process = subprocess.Popen(worker1_command)

    print("[Launcher] Starting Worker Node 2...")
    worker2_process = subprocess.Popen(worker2_command)

    time.sleep(2)

    print("[Launcher] Starting Master Node & Dashboard...")
    try:
        subprocess.run(master_command)
    except KeyboardInterrupt:
        print("\n[Launcher] Shutting down cluster...")
    finally:
        worker1_process.terminate()
        worker2_process.terminate()
        print("[Launcher] Cluster stopped successfully.")


if __name__ == "__main__":
    main()
