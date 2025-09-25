from dotenv import load_dotenv
from googleapiclient.discovery import build
import os
load_dotenv()

api_key = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=api_key)

req = youtube.channels().list(part="id", forHandle="mitocw")
res = req.execute()
if res['items']:
    print("Channel ID for mitocw:", res['items'][0]['id'])

