import os
import json
import logging
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from googleapiclient.discovery import build

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------
# ENV + AWS CONFIG
# ---------------------------
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
CONFIG_FILE = os.getenv("CONFIG_FILE", "config.json")  # local or s3://
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
    config=Config(read_timeout=300, connect_timeout=60)
)

# ---------------------------
# HELPERS
# ---------------------------
def load_config():
    """Load config.json (supports local or s3:// path)."""
    if CONFIG_FILE.startswith("s3://"):
        _, bucket, *key_parts = CONFIG_FILE.split("/")
        key = "/".join(key_parts)
        logger.info(f"Loading config from s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    else:
        logger.info(f"Loading config from {CONFIG_FILE}")
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)

def classify_pdf(link_text: str) -> str:
    txt = link_text.lower()
    if "slide" in txt:
        return "slides"
    elif "note" in txt or "lecture" in txt:
        return "notes"
    return "unknown"

def scrape_ocw_pdfs(course_handle: str):
    """Recursively scrape PDFs/Slides for one OCW course."""
    base_url = f"https://ocw.mit.edu/courses/{course_handle}/"
    visited = set()
    results = []

    def scrape_page(url):
        if url in visited:
            return
        visited.add(url)

        try:
            html = requests.get(url, timeout=20).text
            soup = BeautifulSoup(html, "html.parser")

            # Find PDFs
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.endswith(".pdf"):
                    subtype = classify_pdf(a.get_text())
                    full_url = href if href.startswith("http") else f"https://ocw.mit.edu{href}"
                    results.append({
                        "url": full_url,
                        "type": "pdf",
                        "subtype": subtype,
                        "course_handle": course_handle,
                        "page_url": url,
                        "source": "MIT OCW",
                        "discovered_at": datetime.utcnow().isoformat()
                    })

            # Recurse into subpages
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/courses/") and "/pages/" in href:
                    full_url = f"https://ocw.mit.edu{href}"
                    if full_url not in visited:
                        scrape_page(full_url)

        except Exception as e:
            logger.error(f"Failed to scrape {url}: {e}")

    # Start from the course homepage
    scrape_page(base_url)

    logger.info(f"Found {len(results)} PDFs for {course_handle}")
    return results

def resolve_channel_id(handle: str) -> str:
    """Resolve YouTube handle (@mitocw) to channelId."""
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)
    
    # Remove "@" prefix
    handle = handle.lstrip("@")
    req = youtube.channels().list(part="id", forHandle=handle,        maxResults=1)
    res = req.execute()
    channel_id =  res['items'][0]['id']
    logger.info(f"Found channel_id {channel_id} for {handle}")
    return channel_id

def list_all_playlists(youtube, channel_id):
    """Fetch all playlists for a channel (handle pagination)."""
    playlists = []
    next_page_token = None

    while True:
        req = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        res = req.execute()
        playlists.extend(res.get("items", []))

        next_page_token = res.get("nextPageToken")
        if not next_page_token:
            break

    return playlists


def fetch_youtube_playlists_by_title(handle: str, playlist_titles: list, max_results=20):
    """Fetch videos from specific playlists (using fuzzy title matching)."""
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

    # Step 1: Resolve channel ID
    channel_id =resolve_channel_id(handle=handle)

    # Step 2: Get all playlists (with pagination)
    all_playlists = list_all_playlists(youtube, channel_id)

    found_playlists = {}
    for desired in playlist_titles:
        for pl in all_playlists:
            title = pl["snippet"]["title"]
            if desired.lower() in title.lower():   # fuzzy match
                found_playlists[title] = pl["id"]

    all_videos = []
    for title, playlist_id in found_playlists.items():
        logger.info(f"Fetching playlist '{title}' ({playlist_id}) for {handle}")
        all_videos.extend(fetch_videos_from_playlist(youtube, playlist_id, handle, title, max_results))

    if not found_playlists:
        logger.warning(f"No matching playlists found for {handle} with titles: {playlist_titles}")

    return all_videos


def fetch_videos_from_playlist(youtube, playlist_id, handle, playlist_title, max_results=20):
    """Fetch videos from a specific playlist."""
    req = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,
        maxResults=max_results
    )
    res = req.execute()

    videos = []
    for item in res.get("items", []):
        snippet = item["snippet"]
        video_id = snippet["resourceId"]["videoId"]
        videos.append({
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": snippet["title"],
            "playlist_title": playlist_title,
            "channel_handle": handle,
            "published_at": snippet["publishedAt"],
            "discovered_at": datetime.utcnow().isoformat(),
            "type": "video",
            "subtype": "lecture"
        })
    return videos

def save_to_s3(data, prefix="urls"):
    """Upload discovered resources to S3 as JSON."""
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    key = f"{prefix}/discovered_{timestamp}.json"
    body = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
    logger.info(f"Uploaded discovery results → s3://{S3_BUCKET}/{key}")

# ---------------------------
# MAIN JOB
# ---------------------------
if __name__ == "__main__":
    cfg = load_config()
    all_resources = []

    # OCW courses
    for handle in cfg.get("ocw_courses", []):
        all_resources.extend(scrape_ocw_pdfs(handle))

    # # YouTube channels
    if YOUTUBE_API_KEY:
        for handle, playlist_titles in cfg.get("youtube_playlists", {}).items():
            all_resources.extend(fetch_youtube_playlists_by_title(handle, playlist_titles))

    logger.info(f"Total discovered resources: {len(all_resources)}")
    
    if all_resources:
        save_to_s3(all_resources)
        # print(all_resources)
