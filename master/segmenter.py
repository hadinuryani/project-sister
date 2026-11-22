import os
import re
import subprocess
from typing import List

from common.config import CHUNKS_DIR, DEFAULT_CHUNK_DURATION_SECONDS, OUTPUT_DIR, get_ffmpeg_binary


def get_video_duration(video_path: str) -> float:
    ffmpeg_binary = get_ffmpeg_binary()
    if not ffmpeg_binary:
        raise RuntimeError("FFmpeg executable was not found on this system.")

    command = [ffmpeg_binary, "-i", video_path]
    process = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        errors="replace",
    )

    for line in process.stderr.splitlines():
        if "Duration" in line:
            raw_duration = line.split("Duration:")[1].split(",")[0].strip()
            hours, minutes, seconds = raw_duration.split(":")
            return float(hours) * 3600 + float(minutes) * 60 + float(seconds)

    return 30.0


def split_video_into_chunks(
    video_path: str,
    video_id: str,
    chunk_duration: int = DEFAULT_CHUNK_DURATION_SECONDS,
) -> List[str]:
    ffmpeg_binary = get_ffmpeg_binary()
    if not ffmpeg_binary:
        raise RuntimeError("FFmpeg executable was not found on this system.")

    target_directory = os.path.join(CHUNKS_DIR, video_id)
    os.makedirs(target_directory, exist_ok=True)

    output_pattern = os.path.join(target_directory, "chunk_%03d.mp4")

    command = [
        ffmpeg_binary,
        "-y",
        "-i", video_path,
        "-c", "copy",
        "-map", "0",
        "-segment_time", str(chunk_duration),
        "-f", "segment",
        "-reset_timestamps", "1",
        "-avoid_negative_ts", "make_zero",
        "-break_non_keyframes", "0",
        output_pattern,
    ]

    subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

    raw_chunk_pattern = re.compile(r"^chunk_\d{3}\.mp4$")
    all_files = os.listdir(target_directory)
    matched_chunks = [
        os.path.join(target_directory, filename)
        for filename in all_files
        if raw_chunk_pattern.match(filename)
    ]
    return sorted(matched_chunks)


def merge_chunks(video_id: str, resolution: str, chunk_paths: List[str]) -> str:
    ffmpeg_binary = get_ffmpeg_binary()
    if not ffmpeg_binary:
        raise RuntimeError("FFmpeg executable was not found on this system.")

    target_directory = os.path.join(CHUNKS_DIR, video_id)
    os.makedirs(target_directory, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    concat_manifest_path = os.path.join(target_directory, f"concat_{resolution}.txt")
    with open(concat_manifest_path, "w", encoding="utf-8") as manifest:
        for chunk_path in chunk_paths:
            normalized_path = chunk_path.replace("\\", "/")
            manifest.write(f"file '{normalized_path}'\n")

    output_video_path = os.path.join(OUTPUT_DIR, f"{video_id}_{resolution}.mp4")

    command = [
        ffmpeg_binary,
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", concat_manifest_path,
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-af", "aresample=async=1000",
        "-movflags", "+faststart",
        output_video_path,
    ]

    subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return output_video_path
