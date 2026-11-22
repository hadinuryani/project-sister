import os
import subprocess
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common.config import BASE_DIR, get_ffmpeg_binary


def create_sample_video(
    output_path: str = "sample_test_video.mp4",
    duration_seconds: int = 15,
) -> bool:
    ffmpeg_binary = get_ffmpeg_binary()
    if not ffmpeg_binary:
        print("Error: FFmpeg binary not found on this system.")
        return False

    print(f"Generating test video ({duration_seconds}s, 720p, GOP=30)...")
    command = [
        ffmpeg_binary,
        "-y",
        "-f", "lavfi",
        "-i", f"testsrc=duration={duration_seconds}:size=1280x720:rate=30",
        "-f", "lavfi",
        "-i", f"sine=frequency=1000:duration={duration_seconds}",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-pix_fmt", "yuv420p",
        "-g", "30",
        "-c:a", "aac",
        output_path,
    ]

    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode == 0:
        print(f"Sample video created successfully: {os.path.abspath(output_path)}")
        return True

    print("Failed to generate video:", process.stderr.decode("utf-8", errors="replace"))
    return False


if __name__ == "__main__":
    target_file = os.path.join(BASE_DIR, "sample_test_video.mp4")
    create_sample_video(target_file)
