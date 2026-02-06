import requests
import yaml
import asyncio
from pathlib import Path
from utils import scraping

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


def load_prompt(prompt_name: str, content: str = ""):
    """Load YAML prompt and build universal messages.

    Args:
        prompt_name: 'article_analyzer'
        content: Article text for user message

    Returns:
        dict: OpenAI-ready config
    """
    prompt_file = PROMPTS_DIR / f"{prompt_name}.yaml"

    with open(prompt_file, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    len_content = len(content)

    user_template = config.get("user_template", "Process this content:\n\n{content}")

    user_message = user_template.format(
        len_content=len_content, content=content[:8000]  # Truncate per token limit
    )

    messages = [
        {"role": "system", "content": config["system_prompt"]},
        {"role": "user", "content": user_message},
    ]

    config["data"]["messages"] = messages

    # print(config["data"]["messages"])

    return config


def call_openai(api_key: str, content: str, prompt_name: str = "article_analyzer"):
    """Analyze using YAML prompt

    Args:
        api_key: OpenAI API key
        content: Article text
        prompt_name: Prompt YAML file name without extension

    Returns:
        str: LLM response content
    """
    prompt_config = load_prompt(prompt_name, content)

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=prompt_config["data"],
        timeout=45,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


def analyze_article(
    oai_key: str,
    raw_cookies: list[dict],
    url: str,
    min_chars: int = 1000,
    prompt_name: str = "250109_dq_do_analyzer",
) -> str:
    """Analyzes Medium article with LLM (assumes env/cookies pre-loaded).

    Args:
        oai_key (str): OpenAI API key.
        raw_cookies (list[dict]): Loaded Medium cookies.
        url (str): Medium article URL.
        min_chars (int, optional): Skip if content < this.
        prompt_name (str, optional): LLM prompt name.

    Returns:
        str: GPT analysis result.

    Raises:
        ValueError: Content too short.
    """
    print(f"Analyzing {url}")

    # Async scrape
    content = asyncio.run(scraping.fetch_medium_article(raw_cookies, url))
    print(f"Fetched {len(content):,} chars")

    # Min length
    if len(content) < min_chars:
        raise ValueError(f"Content too short: {len(content)} < {min_chars}")

    print("GPT analyzing...")
    result = call_openai(oai_key, content, prompt_name=prompt_name)

    print("FINAL RESULT:")
    return result
