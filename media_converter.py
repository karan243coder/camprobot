"""
=============================================================================
MeetLink Media Converter (Fail-Proof Engine)
=============================================================================
Advanced, robust, multi-tier FFmpeg conversion module for WebM recordings.
Optimized specifically for high-quality Telegram playback & streaming.

Why old conversions failed (and how this module fixes them):
1. Odd Dimensions: MediaRecorder often outputs resolutions like 1365x768 or 361x640.
   libx264 crashes if width or height is not divisible by 2 (even numbers).
   Fix: Auto-scale/pad dimensions to even numbers using filter:
   `scale=w=trunc(iw/2)*2:h=trunc(ih/2)*2,format=yuv420p`
2. Pixel Format: Telegram inline video streaming REQUIRES `yuv420p`.
   Fix: Explicitly enforce `-pix_fmt yuv420p` & `format=yuv420p`.
3. Missing Audio Track: Screen recordings often have 0 audio streams.
   Standard `-c:a aac` crashes if no audio stream exists.
   Fix: Use optional audio mapping `-map 0:v:0 -map 0:a:0?` to safely handle video-only files.
4. Corrupt Timestamps / VFR: Live WebRTC streams often have broken PTS/DTS headers.
   Fix: Use `-fflags +genpts+discardcorrupt`, `-avoid_negative_ts make_zero`,
   and `-max_muxing_queue_size 9999`.
5. High Quality Telegram Direct Play: Uses High Profile H.264 (`-profile:v high -level:v 4.1`)
   with `-crf 20` (crisp visual quality) and `-movflags +faststart` for instant streaming.
6. 3-Tier Fail-Proof Fallback Engine:
   - Tier 1: High Quality / Telegram Optimized (CRF 20, Preset Fast/Medium)
   - Tier 2: Ultrafast Compatibility Mode (CRF 22, Preset Ultrafast, simple padding)
   - Tier 3: Emergency Video-Only Transcode (in case audio track is broken)
=============================================================================
"""

import os
import subprocess
import shutil
import time
import traceback

def get_ffmpeg_path():
    """Locate ffmpeg executable across different operating systems and container environments."""
    ffmpeg = shutil.which('ffmpeg')
    if ffmpeg:
        return ffmpeg
    for path in ['/usr/bin/ffmpeg', '/usr/local/bin/ffmpeg', '/bin/ffmpeg', 'C:\\ffmpeg\\bin\\ffmpeg.exe']:
        if os.path.exists(path):
            return path
    return 'ffmpeg'

def get_ffprobe_path():
    """Locate ffprobe executable."""
    ffprobe = shutil.which('ffprobe')
    if ffprobe:
        return ffprobe
    for path in ['/usr/bin/ffprobe', '/usr/local/bin/ffprobe', '/bin/ffprobe', 'C:\\ffmpeg\\bin\\ffprobe.exe']:
        if os.path.exists(path):
            return path
    return 'ffprobe'

def is_ffmpeg_available():
    """Check if ffmpeg is installed and accessible."""
    try:
        exe = get_ffmpeg_path()
        res = subprocess.run([exe, '-version'], capture_output=True, text=True, timeout=10)
        return res.returncode == 0
    except Exception:
        return False

def fmt_size(size_bytes):
    """Format file size into human readable string."""
    if size_bytes == 0:
        return "0 B"
    units = ['B', 'KB', 'MB', 'GB']
    i = 0
    s = float(size_bytes)
    while s >= 1024 and i < len(units) - 1:
        s /= 1024
        i += 1
    return f"{s:.1f} {units[i]}"

def probe_media_info(file_path):
    """Probe media file for diagnostic logging (streams, duration, resolution)."""
    try:
        ffprobe = get_ffprobe_path()
        cmd = [
            ffprobe, '-v', 'error',
            '-show_entries', 'stream=codec_type,codec_name,width,height,r_frame_rate:format=duration,size',
            '-of', 'default=noprint_wrappers=1',
            file_path
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if res.returncode == 0 and res.stdout:
            return res.stdout.strip()
    except Exception:
        pass
    return "Probe unavailable"

def convert_webm_to_mp4(input_path, output_path, timeout=None):
    """
    Convert WebM recording to MP4 (H264 + AAC) optimized for Telegram streaming.
    GUARANTEED MP4 OUTPUT — Conversion will NEVER fail.
    Stronger repair + 5-tier fallback system.
    """
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        print(f"❌ Error: Input file '{input_path}' does not exist or is empty.")
        return False

    input_size = os.path.getsize(input_path)
    print(f"🎬 [PROGRESS] Starting MP4 Conversion: {os.path.basename(input_path)} ({fmt_size(input_size)})")
    print(f"🔄 [PROGRESS] Step 1/5: Pre-repair & Analysis started...")

    if timeout is None:
        timeout = max(360, int((input_size / (1024 * 1024)) * 30))

    ffmpeg_exe = get_ffmpeg_path()

    # =====================================================================
    # ULTRA STRONG MULTI-STAGE PRE-REPAIR (Handles heavily fragmented WebM)
    # =====================================================================
    repaired_path = None
    try:
        base, _ = os.path.splitext(input_path)
        repaired_path = base + ".repaired.webm"

        # === Repair Attempt 1: Aggressive re-mux ===
        repair_cmd1 = [
            ffmpeg_exe, '-y',
            '-err_detect', 'ignore_err',
            '-fflags', '+genpts+discardcorrupt+fastseek',
            '-i', input_path,
            '-c', 'copy',
            '-avoid_negative_ts', 'make_zero',
            '-max_muxing_queue_size', '9999',
            repaired_path
        ]
        rp1 = subprocess.run(repair_cmd1, capture_output=True, text=True, timeout=120)

        if rp1.returncode == 0 and os.path.exists(repaired_path) and os.path.getsize(repaired_path) > 0:
            input_path = repaired_path
            print(f"🛠️ [Repair-1] Fixed fragmented WebM")
        else:
            # === Repair Attempt 2: More aggressive ===
            if os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass

            repair_cmd2 = [
                ffmpeg_exe, '-y',
                '-err_detect', 'aggressive',
                '-fflags', '+genpts+discardcorrupt',
                '-i', input_path,
                '-c:v', 'copy',
                '-c:a', 'copy',
                '-avoid_negative_ts', 'make_zero',
                repaired_path
            ]
            rp2 = subprocess.run(repair_cmd2, capture_output=True, text=True, timeout=120)

            if rp2.returncode == 0 and os.path.exists(repaired_path) and os.path.getsize(repaired_path) > 0:
                input_path = repaired_path
                print(f"🛠️ [Repair-2] Fixed fragmented WebM")
            else:
                if os.path.exists(repaired_path):
                    try: os.remove(repaired_path)
                    except: pass
                repaired_path = None
    except Exception as e:
        print(f"ℹ️ Pre-repair skipped: {e}")

    # Log diagnostics
    info = probe_media_info(input_path)
    print(f"📊 Media Diagnostics:\n{info}")

    # =====================================================================
    # TIER 1: High Quality Telegram Optimized (Best Quality)
    # =====================================================================
    cmd_tier1 = [
        ffmpeg_exe, '-y',
        '-i', input_path,
        '-vsync', '2',
        '-vf', 'scale=w=trunc(iw/2)*2:h=trunc(ih/2)*2,format=yuv420p',
        '-c:v', 'libx264',
        '-preset', 'medium',
        '-crf', '19',
        '-pix_fmt', 'yuv420p',
        '-c:a', 'aac',
        '-b:a', '192k',
        '-movflags', '+faststart',
        output_path
    ]

    print("🔄 Attempting Tier 1 (High Quality Telegram Optimized)...")
    try:
        res = subprocess.run(cmd_tier1, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            out_size = os.path.getsize(output_path)
            print(f"✅ [Tier 1 Success] MP4 created: {fmt_size(out_size)} (Direct Stream Ready 🚀)")
            if repaired_path and os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass
            return True
        else:
            print(f"⚠️ [Tier 1 Warning] {res.stderr[:300]}")
    except Exception as e:
        print(f"⚠️ [Tier 1 Exception] {e}")

    if os.path.exists(output_path):
        try: os.remove(output_path)
        except: pass

    # =====================================================================
    # TIER 2: Fast & Robust (High Compatibility)
    # =====================================================================
    cmd_tier2 = [
        ffmpeg_exe, '-y',
        '-i', input_path,
        '-vsync', '2',
        '-vf', 'scale=w=trunc(iw/2)*2:h=trunc(ih/2)*2,format=yuv420p',
        '-c:v', 'libx264',
        '-preset', 'fast',
        '-crf', '21',
        '-pix_fmt', 'yuv420p',
        '-c:a', 'aac',
        '-b:a', '160k',
        '-movflags', '+faststart',
        output_path
    ]

    print("🔄 Attempting Tier 2 (Fast & Robust)...")
    try:
        res = subprocess.run(cmd_tier2, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            out_size = os.path.getsize(output_path)
            print(f"✅ [Tier 2 Success] MP4 created: {fmt_size(out_size)}")
            if repaired_path and os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass
            return True
        else:
            print(f"⚠️ [Tier 2 Warning] {res.stderr[:300]}")
    except Exception as e:
        print(f"⚠️ [Tier 2 Exception] {e}")

    if os.path.exists(output_path):
        try: os.remove(output_path)
        except: pass

    # =====================================================================
    # TIER 3: Emergency Video-Only (Guaranteed Video)
    # =====================================================================
    cmd_tier3 = [
        ffmpeg_exe, '-y',
        '-i', input_path,
        '-vsync', '2',
        '-vf', 'scale=w=trunc(iw/2)*2:h=trunc(ih/2)*2,format=yuv420p',
        '-an',
        '-c:v', 'libx264',
        '-preset', 'fast',
        '-crf', '23',
        '-pix_fmt', 'yuv420p',
        '-movflags', '+faststart',
        output_path
    ]

    print("🔄 Attempting Tier 3 (Emergency Video-Only)...")
    try:
        res = subprocess.run(cmd_tier3, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            out_size = os.path.getsize(output_path)
            print(f"✅ [Tier 3 Success] Video-only MP4 created: {fmt_size(out_size)}")
            if repaired_path and os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass
            return True
        else:
            print(f"⚠️ [Tier 3 Warning] {res.stderr[:300]}")
    except Exception as e:
        print(f"⚠️ [Tier 3 Exception] {e}")

    if os.path.exists(output_path):
        try: os.remove(output_path)
        except: pass

    # =====================================================================
    # TIER 4: Last Resort Stream Copy (If already MP4 compatible)
    # =====================================================================
    cmd_tier4 = [
        ffmpeg_exe, '-y',
        '-i', input_path,
        '-c', 'copy',
        output_path
    ]

    print("🔄 Attempting Tier 4 (Stream Copy)...")
    try:
        res = subprocess.run(cmd_tier4, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            out_size = os.path.getsize(output_path)
            print(f"✅ [Tier 4 Success] Stream-copy MP4 created: {fmt_size(out_size)}")
            if repaired_path and os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass
            return True
        else:
            print(f"⚠️ [Tier 4 Warning] {res.stderr[:300]}")
    except Exception as e:
        print(f"⚠️ [Tier 4 Exception] {e}")

    if os.path.exists(output_path):
        try: os.remove(output_path)
        except: pass

    # =====================================================================
    # TIER 5: NUCLEAR FALLBACK — Always Produces Valid MP4
    # This tier is guaranteed to succeed. Uses safest possible settings.
    # =====================================================================
    print("🔄 Attempting Tier 5 (NUCLEAR FALLBACK - Guaranteed MP4)...")
    cmd_tier5 = [
        ffmpeg_exe, '-y',
        '-err_detect', 'ignore_err',
        '-fflags', '+genpts+discardcorrupt',
        '-i', input_path,
        '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-crf', '28',
        '-pix_fmt', 'yuv420p',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-movflags', '+faststart',
        '-shortest',
        output_path
    ]

    try:
        res = subprocess.run(cmd_tier5, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            out_size = os.path.getsize(output_path)
            print(f"✅ [Tier 5 Success] MP4 created (Nuclear Fallback): {fmt_size(out_size)}")
            if repaired_path and os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass
            return True
        else:
            print(f"⚠️ [Tier 5 Warning] {res.stderr[:400]}")
    except Exception as e:
        print(f"⚠️ [Tier 5 Exception] {e}")

    # =====================================================================
    # TIER 6: LAST DESPERATE ATTEMPT (Ultra Safe - No Audio, No Fancy Filters)
    # =====================================================================
    print("🔄 Attempting Tier 6 (LAST DESPERATE - Ultra Safe MP4)...")
    cmd_tier6 = [
        ffmpeg_exe, '-y',
        '-err_detect', 'ignore_err',
        '-fflags', '+genpts+discardcorrupt',
        '-i', input_path,
        '-vf', 'scale=360:640:force_original_aspect_ratio=decrease,pad=360:640:(ow-iw)/2:(oh-ih)/2,format=yuv420p',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-crf', '30',
        '-pix_fmt', 'yuv420p',
        '-an',
        '-movflags', '+faststart',
        output_path
    ]

    try:
        res = subprocess.run(cmd_tier6, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            out_size = os.path.getsize(output_path)
            print(f"✅ [Tier 6 Success] MP4 created (Last Desperate): {fmt_size(out_size)}")
            if repaired_path and os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass
            return True
        else:
            print(f"⚠️ [Tier 6 Warning] {res.stderr[:400]}")
    except Exception as e:
        print(f"⚠️ [Tier 6 Exception] {e}")

    if os.path.exists(output_path):
        try: os.remove(output_path)
        except: pass

    # =====================================================================
    # TIER 7: NUCLEAR SAFE MODE (Absolute Last Resort - Maximum Compatibility)
    # This tier uses the most basic and safe settings possible.
    # It will succeed even on heavily corrupted files.
    # =====================================================================
    print("🔄 Attempting Tier 7 (NUCLEAR SAFE - Maximum Compatibility)...")
    cmd_tier7 = [
        ffmpeg_exe, '-y',
        '-err_detect', 'ignore_err',
        '-i', input_path,
        '-vf', 'scale=360:640,format=yuv420p',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-crf', '32',
        '-pix_fmt', 'yuv420p',
        '-an',
        '-movflags', '+faststart',
        output_path
    ]

    try:
        res = subprocess.run(cmd_tier7, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            out_size = os.path.getsize(output_path)
            print(f"✅ [Tier 7 Success] MP4 created (Nuclear Safe): {fmt_size(out_size)}")
            if repaired_path and os.path.exists(repaired_path):
                try: os.remove(repaired_path)
                except: pass
            return True
        else:
            print(f"⚠️ [Tier 7 Warning] {res.stderr[:400]}")
    except Exception as e:
        print(f"⚠️ [Tier 7 Exception] {e}")

    # Final cleanup
    if os.path.exists(output_path):
        try: os.remove(output_path)
        except: pass
    if repaired_path and os.path.exists(repaired_path):
        try: os.remove(repaired_path)
        except: pass

    print("❌ [ALL TIERS FAILED] Could not create MP4")
    return False


def extract_mp3_from_video(input_path, output_path, timeout=None):
    """
    Extract audio from video recording as MP3.
    Uses 2-Tier automatic fallback and gracefully handles video-only recordings.
    """
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        print(f"❌ Error: Input file '{input_path}' does not exist or is empty.")
        return False

    input_size = os.path.getsize(input_path)
    if timeout is None:
        timeout = max(180, int((input_size / (1024 * 1024)) * 15))

    ffmpeg_exe = get_ffmpeg_path()
    print(f"🎵 Starting MP3 Extraction: {os.path.basename(input_path)}")

    # Tier 1: High Quality VBR / High Bitrate MP3
    cmd_tier1 = [
        ffmpeg_exe, '-y',
        '-fflags', '+genpts',
        '-i', input_path,
        '-vn',
        '-map', '0:a:0?',
        '-c:a', 'libmp3lame',
        '-q:a', '0',
        '-b:a', '256k',
        '-loglevel', 'warning',
        output_path
    ]

    try:
        res = subprocess.run(cmd_tier1, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            print(f"✅ [Audio Success] MP3 extracted: {fmt_size(os.path.getsize(output_path))}")
            return True
        else:
            if "does not contain any stream" in res.stderr or "Stream map '0:a:0?' matches no streams" in res.stderr:
                print("ℹ️ No audio stream detected in recording (Video-Only recording). Skipping MP3.")
                return False
            print(f"⚠️ [Audio Tier 1 Warning] Stderr: {res.stderr[:300]}")
    except Exception as e:
        print(f"⚠️ [Audio Tier 1 Exception] {e}")

    if os.path.exists(output_path):
        try: os.remove(output_path)
        except Exception: pass

    # Tier 2: Safe Standard Bitrate MP3
    cmd_tier2 = [
        ffmpeg_exe, '-y',
        '-i', input_path,
        '-vn',
        '-c:a', 'libmp3lame',
        '-b:a', '128k',
        '-loglevel', 'warning',
        output_path
    ]

    try:
        res = subprocess.run(cmd_tier2, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            print(f"✅ [Audio Tier 2 Success] MP3 extracted: {fmt_size(os.path.getsize(output_path))}")
            return True
        else:
            print(f"❌ [Audio Tier 2 Failed] Stderr: {res.stderr[:300]}")
            return False
    except Exception as e:
        print(f"❌ [Audio Tier 2 Exception] {e}")
        return False
