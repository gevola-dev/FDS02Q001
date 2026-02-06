import os
import json
from dotenv import load_dotenv
from utils import media
import sqlite3
import pandas as pd

print(sqlite3.sqlite_version)  # Output: 3.45.x o simile
conn = sqlite3.connect(':memory:')  # Test in-memory
conn.close()
print("✅ SQLite pronto!")

# C:\Users\g.evola\repo\my_notion\.env
# C:\Users\Work\Documents\GitHub\my_notion\.env
dotenv_path = r"C:\Users\g.evola\repo\my_notion\.env"
load_dotenv(dotenv_path, override=True)

COOKIES_PATH = r"C:\Users\g.evola\repo\my_notion\src\conf\medium_cookies.json"
OAI_API_KEY = os.getenv("OAI_API_KEY")

if not OAI_API_KEY:
    raise RuntimeError("Missing OAI_API_KEY")

raw_cookies = []
if os.path.exists(COOKIES_PATH):
    with open(COOKIES_PATH, encoding="utf-8") as f:
        raw_cookies = json.load(f)
    print(f"Cookies: {len(raw_cookies)}")

# MAIN SCRIPT

if __name__ == "__main__":

    print("Fetching existing articles from Notion...")
    existing_links = ["https:dvvsfbdbnd"]

    # Iterate over all RSS feeds
    for feed in media.RSS_FEEDS:

        # Extract parameters for Notion
        feed_url = feed["url"]
        feed_cap = feed["cap"]
        topic_id = feed["topic_id"]
        creator_id = feed["creator_id"]
        publication_id = feed["publication_id"]
        # Extract skip prefixes, default to empty list if not provided
        skip_prefixes = feed.get("skip", [])

        do_llm_analysis = feed.get("llm_analysis", False)

        print(f"\nFetching articles from RSS: {feed_url} (Cap: {feed_cap})")
        entries = media.parse_rss_feed(feed_url)
        valid_entries = media.filter_valid_articles(
            entries, existing_links, feed_cap, skip_prefixes=skip_prefixes
        )

        #feed_articles = [
        #    notion.resource_db_formatting(entry) for entry in valid_entries
        #]

        # Check if feed_articles is empty
        #if not feed_articles:
        #    print("No articles found for this feed. Skipping...")
        #    continue

        print(f"Try to insert {feed_cap} new articles for this feed...")
        # add articles

    print("All articles processed!")

