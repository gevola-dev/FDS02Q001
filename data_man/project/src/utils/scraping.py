import re
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError, Error


PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


def format_playwright_cookies(cookies: list):
    """Convert EditThisCookie JSON format to Playwright cookie format.

    Args:
        cookies (list): Raw cookies from EditThisCookie export

    Returns:
        list: Playwright-compatible cookies with fixed sameSite values
    """
    pw_cookies = []

    for cookie in cookies:
        pw_cookie = {
            "name": cookie["name"],
            "value": cookie["value"],
            "domain": cookie["domain"],
            "path": cookie.get("path", "/"),
        }
        same_site = cookie.get("sameSite", "Lax")
        if same_site in ["Strict", "Lax", "None"]:
            pw_cookie["sameSite"] = same_site
        pw_cookies.append(pw_cookie)

    return pw_cookies


def safe_cleanup(raw_content: str):
    """Safe cleanup with failsafe restore.

    Args:
        raw_content (str): Original scraped content

    Returns:
        str: Cleaned content or original if cleanup too aggressive
    """
    original_len = len(raw_content)

    # Basic cleanup
    content = re.sub(r"\n{3,}", "\n\n", raw_content)
    content = re.sub(r"\s+", " ", content)
    content = content.strip()

    final_len = len(content)
    print(f"original len: {original_len} chars, cleaned len: {final_len} chars")

    # Failsafe: if too aggressive, restore original
    if final_len < 1000 and original_len > 3000:
        print("Cleanup too aggressive → RESTORE ORIGINAL")
        return raw_content[:15000]
    else:
        return content[:15000]


async def fetch_medium_article(raw_cookies: list, url: str):
    """Fetch full Medium article bypassing paywall with cookies.
       Async function so the await functions put in parallel the requests if needed.

    Args:
        url (str): Medium article URL

    Returns:
        str: Cleaned article content (min 1000 chars)
    """
    pw_cookies = format_playwright_cookies(raw_cookies)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        await context.add_cookies(pw_cookies)
        page = await context.new_page()

        print("Loading the article page...")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)

        content = ""
        selectors = [
            "article",
            "main",
            ".postArticle-content",
            ".pw-article-body",
            "[data-testid='post-article-content']",
        ]  # TO BE EXPANDED

        for selector in selectors:
            try:
                element = await page.wait_for_selector(selector, timeout=4000)
                raw_content = await element.text_content()
                if raw_content and len(raw_content.strip()) > 1500:
                    # Found sufficient content
                    content = raw_content
                    break
            except (TimeoutError, Error):
                print(f"Selector failed: {selector}")
                continue

        # Fallback to raw article content
        raw_content = content or await page.text_content("body")

        # Cleanup article content
        cleaned_content = safe_cleanup(raw_content)

        await browser.close()
        return cleaned_content
