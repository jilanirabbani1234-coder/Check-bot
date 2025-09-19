#!/usr/bin/env python3
"""
Minimal sequential downloader bot.

Features:
- Accepts a text file containing links (one per line or separated).
- Processes links in order (videos & PDFs mixed).
- Uses yt-dlp to download streaming manifests (mpd/m3u8/etc).
- Uses ffmpeg to remove video metadata (fast copy).
- Uses PyPDF2 to rewrite PDFs with metadata removed.
- Uploads cleaned files to a Telegram channel (channel set by /setchannel).
- Keeps simple per-user channel mapping in channels.json (local).
- Does NOT store or require token in code. Use env var BOT_TOKEN.
"""
import os
import json
import shlex
import shutil
import tempfile
import subprocess
from pathlib import Path
from typing import List
import logging
import re

import requests
from PyPDF2 import PdfReader, PdfWriter
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# --------- CONFIG ----------
BOT_TOKEN_ENV = "BOT_TOKEN"  # provide this in Render/GitHub secret
CHANNEL_MAP_FILE = "channels.json"
MAX_TELEGRAM_FILE = 2 * 1024 * 1024 * 1024  # 2 GB
# --------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_channel_map() -> dict:
    if os.path.exists(CHANNEL_MAP_FILE):
        try:
            with open(CHANNEL_MAP_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}


def save_channel_map(m: dict):
    with open(CHANNEL_MAP_FILE, "w", encoding="utf-8") as fh:
        json.dump(m, fh)


def sanitize_name(s: str) -> str:
    s = s.strip()
    # remove extension if present
    s = re.sub(r"\.(mp4|mkv|webm|mpd|m3u8|pdf|mov|ts|avi)$", "", s, flags=re.I)
    # keep alnum and few safe chars
    safe = "".join(c if (c.isalnum() or c in " -_") else "_" for c in s)
    return safe[:120]


def parse_links_from_text(text: str) -> List[str]:
    # extract http/https links and also file:// if any
    pattern = re.compile(r"(https?://[^\s'\"<>]+)")
    found = pattern.findall(text)
    # If none found, try splitting by whitespace to pick tokens that look like links
    if not found:
        tokens = re.split(r"\s+", text)
        found = [t for t in tokens if t.startswith("http")]
    # keep order and unique? We preserve order and duplicates too (user wanted sequence)
    return found


def is_pdf_link(url: str) -> bool:
    return url.lower().split("?")[0].endswith(".pdf")


def looks_like_video(url: str) -> bool:
    lower = url.lower().split("?")[0]
    if lower.endswith((".mp4", ".mkv", ".webm", ".mov", ".ts", ".avi")):
        return True
    if "master.mpd" in lower or lower.endswith(".mpd") or lower.endswith(".m3u8"):
        return True
    # otherwise assume it's video if not pdf and not obviously something else
    return not is_pdf_link(url)


async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = (
        "Hello! Send me a text file that contains links (one per line) in the exact order "
        "you want them uploaded. Links can be PDFs and streaming manifests (master.mpd, .m3u8, direct .mp4). \n\n"
        "Before sending the links file, set the upload channel using:\n"
        "/setchannel <channel_id_or_@username>\n\n"
        "Example:\n/setchannel -1001234567890\n\n"
        "Bot will process items sequentially and upload cleaned files to that channel. "
        "I do not store your bot token—set it as BOT_TOKEN environment variable when deploying."
    )
    await update.message.reply_text(txt)


async def setchannel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if not ctx.args:
        await update.message.reply_text("Usage: /setchannel <channel_id_or_@username>")
        return
    channel = ctx.args[0].strip()
    m = load_channel_map()
    m[user_id] = channel
    save_channel_map(m)
    await update.message.reply_text(f"Channel saved for you: {channel}\nMake sure the bot is admin in that channel.")


async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    m = load_channel_map()
    if user_id not in m:
        await update.message.reply_text("No channel set for you. Use /setchannel <channel_id_or_@username> first.")
        return
    channel = m[user_id]
    doc = update.message.document
    if not doc:
        await update.message.reply_text("Please send a text file containing links.")
        return

    # accept any text-like document
    msg = await update.message.reply_text("Downloading the links file...")
    tmpdir = tempfile.mkdtemp(prefix="mpdbot_")
    try:
        file_path = Path(tmpdir) / doc.file_name
        await doc.get_file().download_to_drive(str(file_path))
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        links = parse_links_from_text(text)
        if not links:
            await msg.edit_text("No links found in the file.")
            return
        await msg.edit_text(f"Found {len(links)} links. Starting sequential processing...")
        serial = 1
        for url in links:
            # brief status update
            await msg.edit_text(f"Processing #{serial}: {url}")
            basename = sanitize_name(Path(url.split("?")[0]).name)
            # fallback if basename empty
            if not basename:
                basename = f"item_{serial}"
            name_with_serial = f"{serial}{basename}"
            try:
                if is_pdf_link(url):
                    produced = await download_and_clean_pdf(url, tmpdir, name_with_serial)
                else:
                    produced = await download_and_clean_video(url, tmpdir, name_with_serial)
            except Exception as e:
                logger.exception("Error processing %s", url)
                await update.message.reply_text(f"Failed to process {url}: {e}")
                serial += 1
                continue

            if not produced or not os.path.exists(produced):
                await update.message.reply_text(f"Processing failed for {url}.")
                serial += 1
                continue

            fsize = os.path.getsize(produced)
            if fsize > MAX_TELEGRAM_FILE:
                await update.message.reply_text(f"Skipping upload for {name_with_serial}: file too large ({fsize} bytes).")
                # optionally upload external link or notify
                serial += 1
                continue

            # upload to channel as document (generic) so pdf/video both supported
            await msg.edit_text(f"Uploading {name_with_serial} ({fsize//1024//1024} MB) to {channel} ...")
            with open(produced, "rb") as fh:
                await ctx.bot.send_document(chat_id=channel, document=fh, filename=os.path.basename(produced),
                                            caption=name_with_serial)
            # cleanup produced file for space
            try:
                os.remove(produced)
            except Exception:
                pass
            serial += 1

        await msg.edit_text("All items processed.")
    finally:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass


async def download_and_clean_pdf(url: str, tmpdir: str, outname: str) -> str:
    """
    Download PDF via HTTP, rewrite it to remove metadata, return path to cleaned pdf.
    """
    local_raw = os.path.join(tmpdir, f"{outname}.raw.pdf")
    local_clean = os.path.join(tmpdir, f"{outname}.pdf")

    # stream download
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_raw, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    # use PyPDF2 to rewrite and clear metadata
    reader = PdfReader(local_raw)
    writer = PdfWriter()
    for p in reader.pages:
        writer.add_page(p)
    # empty metadata
    writer.add_metadata({})
    with open(local_clean, "wb") as f:
        writer.write(f)

    # remove raw
    try:
        os.remove(local_raw)
    except Exception:
        pass
    return local_clean


async def download_and_clean_video(url: str, tmpdir: str, outname: str) -> str:
    """
    Use yt-dlp to download streaming manifests or direct videos, then run ffmpeg to remove metadata.
    Returns the cleaned filepath.
    """
    # pick a safe output template: outname.%(ext)s
    out_template = os.path.join(tmpdir, f"{outname}.%(ext)s")
    # call yt-dlp via python -m to avoid PATH problems
    cmd = ["python", "-m", "yt_dlp", "-f", "best", "-o", out_template, url]
    logger.info("Running yt-dlp: %s", " ".join(shlex.quote(x) for x in cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(f"yt-dlp failed: {proc.stderr.decode(errors='ignore')[:400]}")

    # find produced file (first match)
    produced = None
    for p in Path(tmpdir).iterdir():
        if p.name.startswith(outname + "."):
            produced = str(p)
            break
    if not produced:
        # try fallback: pick newest file in tmpdir
        files = list(Path(tmpdir).glob("*"))
        if files:
            files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            produced = str(files[0])
    if not produced:
        raise RuntimeError("Downloaded file not found")

    # create cleaned file path
    ext = Path(produced).suffix
    cleaned = os.path.join(tmpdir, f"{outname}{ext}")
    # use ffmpeg to strip metadata (copy streams)
    ffmpeg_cmd = ["ffmpeg", "-y", "-i", produced, "-map_metadata", "-1", "-c", "copy", cleaned]
    logger.info("Running ffmpeg: %s", " ".join(shlex.quote(x) for x in ffmpeg_cmd))
    r = subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        # if copy fails (some containers), fallback to re-encode (slower)
        logger.warning("ffmpeg copy failed, trying re-encode fallback")
        ff_fallback = ["ffmpeg", "-y", "-i", produced, "-map_metadata", "-1", cleaned]
        r2 = subprocess.run(ff_fallback, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if r2.returncode != 0:
            raise RuntimeError("ffmpeg failed to clean metadata")
    # remove original produced file
    try:
        os.remove(produced)
    except Exception:
        pass
    return cleaned


async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Please upload a text file (Document) that contains links in order. Use /setchannel <id> to set upload channel.")


def main():
    token = os.getenv(BOT_TOKEN_ENV)
    if not token:
        raise RuntimeError("Please set BOT_TOKEN environment variable before running.")
    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("setchannel", setchannel_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    logger.info("Bot starting (polling)...")
    app.run_polling()


if __name__ == "__main__":
    main()
