import feedparser
from dateutil import parser
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime, timedelta, timezone


RSS_FEEDS = [
    {
        "url": "https://www.liberioltreleillusioni.it/rss-feed?type=9818",
        "cap": 3,
        "topic_id": "ffbb657eb0c443f79523753547e16313",
        "creator_id": "1864c90d52dd8016961be693bbb16798",
        "publication_id": "2074c90d52dd80218e7de2de0807b9be",
    },
    {
        "url": "https://medium.com/feed/tag/data-quality",
        "cap": 5,
        "topic_id": "6507c6fc473a4f2ea5558ef132f67d8f",
        "creator_id": "1694c90d52dd80beb889e455f98daf18",
        "publication_id": "2ad4c90d52dd80c09dc7e985e9420123",
        "llm_analysis": True,
    },
    {
        "url": "https://medium.com/feed/tag/data-observability",
        "cap": 5,
        "topic_id": "80b5507f579e4af5a60b16e9c83474c5",
        "creator_id": "1694c90d52dd80beb889e455f98daf18",
        "publication_id": "2ad4c90d52dd808c94f9f5a9e6239e27",
        "llm_analysis": True,
    },
    {
        "url": "https://medium.com/feed/data-reply-it-datatech",
        "cap": 3,
        "topic_id": "9fef5d736db54922a4fb4452aa87428e",
        "creator_id": "1694c90d52dd80beb889e455f98daf18",
        "publication_id": "2074c90d52dd807c8230e48b3f1e3ddf",
    },
    {
        "url": "https://www.dedaloinvest.com/w-blog?format=feed&type=rss",
        "cap": 1,
        "topic_id": "efed055bed0744588052b3988d47bd7c",
        "creator_id": "1874c90d52dd80b494bbf846a357920b",
        "publication_id": "2064c90d52dd8069ae4ac69232db530b",
    },
    {
        "url": "https://ofdollarsanddata.com/feed/",
        "cap": 1,
        "topic_id": "efed055bed0744588052b3988d47bd7c",
        "creator_id": "1694c90d52dd8033b4c1c4b87be95d5c",
        "publication_id": "2074c90d52dd803b900ee36c7cb0572b",
    },
    {
        "url": "https://rss.app/feeds/HLHhW5lvHRORFWvW.xml",  # Foreign Affairs most read articles
        "cap": 0,
        "topic_id": "ffbb657eb0c443f79523753547e16313",
        "creator_id": "2614c90d52dd801ba686c40503c88c7f",
        "publication_id": "2ad4c90d52dd8095b7ccd0f1797effc6",
    },
    {
        "url": "https://www.youtube.com/feeds/videos.xml?channel_id=UCNJ1Ymd5yFuUPtn21xtRbbw",  # AI explaned
        "cap": 1,
        "topic_id": "9fef5d736db54922a4fb4452aa87428e",
        "creator_id": "2064c90d52dd80fb8346e158dd064452",
        "publication_id": "2064c90d52dd8026a0c1c6630d40a3a1",
    },
    {
        "url": "https://www.youtube.com/feeds/videos.xml?channel_id=UCNxDaEFXPIlvqFZgvvk-K_Q",  # astronauticast
        "cap": 1,
        "topic_id": "fff4c90d52dd809fa623f66a358df3ea",
        "creator_id": "1694c90d52dd8037b5f6c2e09528d855",
        "publication_id": "2064c90d52dd80169a26c17d15b6a876",
        "skip": ["ISS Timelapse"],  # Skip entries starting with "ISS Timelapse"
    },
    {
        "url": "https://www.youtube.com/feeds/videos.xml?channel_id=UCrdEJmK5bgFte04-UF7o29Q",  # liberi oltre le illusioni
        "cap": 3,
        "topic_id": "ffbb657eb0c443f79523753547e16313",
        "creator_id": "1864c90d52dd8016961be693bbb16798",
        "publication_id": "2a64c90d52dd8085a7daf3524e0f863d",
        "skip": ["C.F.A", "Daily Spot", "https://www.youtube.com/shorts/"],
    },
    {
        "url": "https://www.youtube.com/feeds/videos.xml?channel_id=UCMOiTfbUXxUFqJJtCQGHrrA",  # michele boldrin
        "cap": 3,
        "topic_id": "ffbb657eb0c443f79523753547e16313",
        "creator_id": "1694c90d52dd80ce9bf0ef7ceda94534",
        "publication_id": "2064c90d52dd804399d5dfe5dde114b9",
        "skip": ["https://www.youtube.com/shorts/"],
    },
    {
        "url": "https://rss.app/feeds/bs4ixDFrPxAZPcOT.xml",  # The batch - deepLearning.AI
        "cap": 0,
        "topic_id": "9fef5d736db54922a4fb4452aa87428e",
        "creator_id": "2064c90d52dd809cb578e7df55adc64c",
        "publication_id": "2064c90d52dd8023af6de041dd1ba5e0",
    },
    {
        "url": "https://www.youtube.com/feeds/videos.xml?channel_id=UCsE_m2z1NrvF2ImeNWh84mw",  # ActiveSelfProtection
        "cap": 3,
        "topic_id": "2c24c90d52dd809fa08cd06b4e7484ba",
        "creator_id": "2c24c90d52dd8036a75ed61f4a3af156",
        "publication_id": "2064c90d52dd804399d5dfe5dde114b9",
        "skip": ["https://www.youtube.com/shorts/"],
    },
    {
        "url": "https://www.youtube.com/feeds/videos.xml?channel_id=UCCxvrGHKqKEEBCWM3oqii9w",  # Corto circuito
        "cap": 1,
        "topic_id": "1764c90d52dd800284bef2c30164c2a7",
        "creator_id": "2cc4c90d52dd80978e43e365a1f85690",
        "publication_id": "2cc4c90d52dd809a98cbfd1f2fc6708d",
    },
]


def normalize_url(url):
    """Normalize URLs for duplicate detection.

    Converts URLs to lowercase, removes query parameters and fragments,
    and preserves only the YouTube video ID when applicable.

    Args:
        url (str): The URL to normalize.

    Returns:
        str: The normalized URL.
    """

    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if "youtube" in parsed.netloc.lower():
        qs = parse_qs(parsed.query)
        video_id = qs.get("v", [])
        if video_id:
            new_query = urlencode({"v": video_id[0]})
        else:
            new_query = ""
        normalized = parsed._replace(query=new_query, fragment="", path=path)
    else:
        normalized = parsed._replace(query="", fragment="", path=path)

    final_url = urlunparse(normalized).lower()

    return final_url


def should_skip_entry(entry, skip_prefixes):
    """Check whether a feed entry should be skipped based on title prefixes.

    Args:
        entry (dict): The RSS feed entry.
        skip_prefixes (list[str]): List of title prefixes to skip.

    Returns:
        bool: True if entry should be skipped, False otherwise.
    """
    title = entry.title if hasattr(entry, "title") else entry.get("title", "")
    return any(title.startswith(prefix) for prefix in skip_prefixes)


def fetch_rss_articles(
    rss_url: str, skip_prefixes: list[str] = None, months_back: int = 3
):
    """Fetches and parses articles from an RSS feed.

    Filters out skipped prefixes and articles older than `months_back` months ago.
    Handles timezone-aware dates, None/malformed dates gracefully. Returns standardized
    dicts for Notion database import.

    Args:
        rss_url (str): RSS feed URL to fetch and parse.
        skip_prefixes (list[str], optional): Title prefixes to skip (via should_skip_entry).
            Defaults to None.
        months_back (int, optional): Keep articles from last N months (approx. 30 days/month).
            Defaults to 6.

    Returns:
        list[dict]: Articles with keys:
            - title (str): Article title.
            - link (str): Article URL.
            - published (str): ISO 8601 datetime (with timezone).
            - summary (str or None): Article summary.
            - author (str): Author name, defaults to "Unknown".

    Raises:
        None: Logs errors (feed.bozo, date parse) and skips invalid entries.
    """
    feed = feedparser.parse(rss_url)
    articles = []
    cutoff_date = (datetime.now() - timedelta(days=30 * months_back)).replace(
        tzinfo=None
    )

    # Check for feed parsing errors
    if feed.bozo:
        print(f"Error parsing feed {rss_url}: {feed.bozo_exception}")
        return articles

    for entry in feed.entries:

        if skip_prefixes and should_skip_entry(entry, skip_prefixes):
            print(f"Skipping article due to prefix match: {entry.title}")
            continue

        published_date_iso = None
        if "published" in entry:

            published_date = parser.parse(entry.published)

            # Normalize published date
            published_date_utc = published_date.astimezone(timezone.utc).replace(
                tzinfo=None
            )

            if published_date_utc < cutoff_date:
                print(
                    f"Skipping article from more than {months_back} months ago: {entry.title} ({published_date.date()})"
                )
                continue

            published_date_iso = published_date.strftime("%Y-%m-%dT%H:%M:%S%z")

        else:
            print(f"Article missing published date, skipping: {entry.title}")
            continue

        articles.append(
            {
                "title": entry.title,
                "link": entry.link,
                "published": published_date_iso,
                "summary": entry.get("summary", None),
                "author": entry.get("author", "Unknown"),
            }
        )

    print(f"Fetched {len(articles)} recent articles.")
    return articles


def parse_rss_feed(rss_url: str) -> list[dict]:
    """Parses RSS/Atom feed and returns raw entries.

    Logs bozo exceptions, returns empty list on parse failure.

    Args:
        rss_url (str): RSS feed URL.

    Returns:
        list[dict]: Raw feedparser entry dicts, or [] on error.
    """
    feed = feedparser.parse(rss_url)
    if feed.bozo:
        print(f"Error parsing {rss_url}: {feed.bozo_exception}")
        return []
    return feed.entries


def filter_valid_articles(
    entries: list[dict],
    existing_links: set[str],
    feed_cap: int,
    skip_prefixes: list[str] | None = None,
    months_back: int = 3,
) -> list[dict]:
    """Filters RSS entries for Notion sync: prefixes, dates, duplicates, cap.

    Validates published date, skips prefixes/old/dups, stops at feed_cap.

    Args:
        entries (list[dict]): Raw feedparser entries.
        existing_links (set[str]): Normalized URLs already in Notion.
        feed_cap (int): Max articles to return.
        skip_prefixes (list[str], optional): Title prefixes to skip.
        months_back (int, optional): Keep last N months (~30 days/month).

    Returns:
        list[dict]: Ready-to-format entries (unique, recent, valid).
    """
    cutoff = (datetime.now() - timedelta(days=30 * months_back)).replace(tzinfo=None)
    valid_entries = []
    processed = 0

    for entry in entries:
        processed += 1
        print(f"Processing {processed}/{len(entries)}: {entry.title[:60]}...")

        # 1. Prefix filter
        if skip_prefixes and should_skip_entry(entry, skip_prefixes):
            print("  → SKIP prefix")
            continue

        # 2. Title/link validation
        if not entry.get("title") or not entry.get("link"):
            print("  → SKIP missing title/link")
            continue

        # 3. Published validation + recency
        if "published" not in entry or not entry.published:
            print("  → SKIP no published")
            continue
        try:
            pub_date = parser.parse(entry.published)
            if pub_date is None:
                print("  → SKIP unparsable date")
                continue
            pub_utc = pub_date.astimezone(timezone.utc).replace(tzinfo=None)
            if pub_utc < cutoff:
                print(f"  → SKIP old ({pub_utc.date()})")
                continue
        except Exception as e:
            print(f"  → SKIP date error: {e}")
            continue

        # 4. Duplicate checks
        normalized_link = normalize_url(entry["link"])
        if normalized_link in existing_links:
            print(f"  → SKIP duplicate: {normalized_link}")
            continue
        # normalze entire list hasnt good performance for large lists
        if normalized_link in {normalize_url(e["link"]) for e in valid_entries}:
            print(f"  → SKIP internal duplicate: {normalized_link}")
            continue

        # 5. Cap check
        if len(valid_entries) >= feed_cap:
            print(f"  → CAP reached ({feed_cap})")
            break

        # Valid!
        valid_entries.append(entry)
        print(f"  → VALID ({len(valid_entries)}/{feed_cap})")

    print(f"Final: {len(valid_entries)} valid articles (cap {feed_cap})")
    return valid_entries
