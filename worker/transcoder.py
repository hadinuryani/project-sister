import os
import subprocess
import time
from typing import Any, Dict

from common.config import RESOLUTIONS, get_ffmpeg_binary


def transcode_video_chunk(
    input_path: str,
    output_path: str,
    target_resolution: str = "720p",
) -> Dict[str, Any]:
    ffmpeg_binary = get_ffmpeg_binary()
    if not ffmpeg_binary:
        raise RuntimeError("FFmpeg executable was not found on this system.")

    profile = RESOLUTIONS.get(target_resolution, RESOLUTIONS["720p"])
    scale_dimension = profile["scale"]
    video_bitrate = profile["video_bitrate"]
    audio_bitrate = profile["audio_bitrate"]

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    video_filter = (
        f"scale={scale_dimension}:force_original_aspect_ratio=decrease,"
        f"pad={scale_dimension}:(ow-iw)/2:(oh-ih)/2"
    )

    command = [
        ffmpeg_binary,
        "-y",
        "-i", input_path,
        "-vf", video_filter,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-b:v", video_bitrate,
        "-r", "30",
        "-g", "30",
        "-keyint_min", "30",
        "-sc_threshold", "0",
        "-c:a", "aac",
        "-b:a", audio_bitrate,
        "-ar", "44100",
        "-ac", "2",
        "-avoid_negative_ts", "make_zero",
        "-max_muxing_queue_size", "1024",
        output_path,
    ]

    start_time = time.time()
    execution = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    duration_ms = int((time.time() - start_time) * 1000)

    if execution.returncode != 0:
        error_details = execution.stderr.decode("utf-8", errors="replace")
        return {
            "success": False,
            "error": error_details[-300:],
            "duration_ms": duration_ms,
        }

    return {
        "success": True,
        "output_path": output_path,
        "target_resolution": target_resolution,
        "duration_ms": duration_ms,
    }
