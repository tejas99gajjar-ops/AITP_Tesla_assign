import os
import json
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import hashlib

# Load API key from environment
YOUTUBE_API_KEY = os.getenv("AIzaSyAKYu_issNMgLc8RZtR70UHhxl3nYWNn7g")
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def extract_video_id_from_url(url):
    """Extract video ID from YouTube URL."""
    if "youtube.com/watch?v=" in url:
        return url.split("v=")[1].split("&")[0]
    elif "youtu.be/" in url:
        return url.split("youtu.be/")[1].split("?")[0]
    return url

def fetch_comments_for_video(video_url, max_results=150):
    """
    Fetch top-voted + newest comments from a YouTube video.
    Returns list of comment dicts.
    """
    video_id = extract_video_id_from_url(video_url)
    comments = []
    
    # Fetch top-voted comments
    request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_id,
        textFormat="plainText",
        order="relevance",
        maxResults=min(100, max_results // 2),
        pageToken=None
    )
    
    while request and len(comments) < max_results:
        response = request.execute()
        
        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comment_dict = {
                "comment_id": item["id"],
                "video_id": video_id,
                "author_name_hash": hashlib.sha256(
                    snippet["authorDisplayName"].encode()
                ).hexdigest()[:16],  # Anonymize
                "comment_text": snippet["textDisplay"],
                "published_at": snippet["publishedAt"],
                "like_count": snippet["likeCount"],
                "reply_count": item["snippet"]["totalReplyCount"],
                "parent_comment_id": None,
                "language_detected": "en",  # Assume English for this project
                "source": "youtube",
                "created_at": datetime.utcnow().isoformat()
            }
            comments.append(comment_dict)
            
            # Get replies to this comment
            if item["snippet"]["totalReplyCount"] > 0:
                for reply in item.get("replies", {}).get("comments", [])[:5]:  # First 5 replies
                    reply_snippet = reply["snippet"]
                    reply_dict = {
                        "comment_id": reply["id"],
                        "video_id": video_id,
                        "author_name_hash": hashlib.sha256(
                            reply_snippet["authorDisplayName"].encode()
                        ).hexdigest()[:16],
                        "comment_text": reply_snippet["textDisplay"],
                        "published_at": reply_snippet["publishedAt"],
                        "like_count": reply_snippet["likeCount"],
                        "reply_count": 0,
                        "parent_comment_id": item["id"],  # Link to parent
                        "language_detected": "en",
                        "source": "youtube",
                        "created_at": datetime.utcnow().isoformat()
                    }
                    comments.append(reply_dict)
        
        # Try next page
        if "nextPageToken" in response:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                textFormat="plainText",
                order="relevance",
                maxResults=100,
                pageToken=response["nextPageToken"]
            )
        else:
            request = None
    
    print(f"✓ Fetched {len(comments)} comments from video {video_id}")
    return comments

def fetch_newest_comments(video_url, max_results=50):
    """Fetch newest comments separately."""
    video_id = extract_video_id_from_url(video_url)
    comments = []
    
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        textFormat="plainText",
        order="time",  # Newest first
        maxResults=min(100, max_results),
        pageToken=None
    )
    
    while request and len(comments) < max_results:
        response = request.execute()
        
        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comment_dict = {
                "comment_id": item["id"],
                "video_id": video_id,
                "author_name_hash": hashlib.sha256(
                    snippet["authorDisplayName"].encode()
                ).hexdigest()[:16],
                "comment_text": snippet["textDisplay"],
                "published_at": snippet["publishedAt"],
                "like_count": snippet["likeCount"],
                "reply_count": item["snippet"]["totalReplyCount"],
                "parent_comment_id": None,
                "language_detected": "en",
                "source": "youtube",
                "created_at": datetime.utcnow().isoformat()
            }
            comments.append(comment_dict)
        
        if "nextPageToken" in response:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                textFormat="plainText",
                order="time",
                maxResults=100,
                pageToken=response["nextPageToken"]
            )
        else:
            request = None
    
    return comments

def main():
    # Video URLs from curation log
    video_urls = [
        "https://youtube.com/watch?v=abc123def456",  # Replace with real URLs
        "https://youtube.com/watch?v=ghi789jkl012",
        # ... 2 more videos
    ]
    
    all_comments = []
    
    for url in video_urls:
        print(f"Fetching comments from: {url}")
        # Relevant comments
        relevant = fetch_comments_for_video(url, max_results=75)
        all_comments.extend(relevant)
        # Newest comments
        newest = fetch_newest_comments(url, max_results=75)
        all_comments.extend(newest)
    
    # Remove duplicates (same comment_id)
    unique_comments = {c["comment_id"]: c for c in all_comments}
    print(f"✓ Total unique comments: {len(unique_comments)}")
    
    # Export to CSV
    df = pd.DataFrame(list(unique_comments.values()))
    df.to_csv("data/comments.csv", index=False)
    print(f"✓ Exported comments to data/comments.csv")
    
    # Print sample
    print("\nSample comments:")
    print(df[["comment_text", "like_count"]].head(5))

if __name__ == "__main__":
    main()
