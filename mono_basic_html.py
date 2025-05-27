import sys
import argparse
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

try:
    import lxml.html  # type: ignore
    HAS_LXML = True
except Exception:  # pragma: no cover - optional dependency
    HAS_LXML = False


class BasicHTMLScraper:
    """Simple scraper for extracting data using CSS selectors or XPath."""

    def __init__(self):
        self.session = self._setup_session()

    def _setup_session(self):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                )
            }
        )
        return session

    def fetch_page(self, url: str) -> str | None:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # pragma: no cover - network errors
            print(f"Error fetching {url}: {exc}", file=sys.stderr)
            return None

    def extract_css(self, html: str, selectors: list[str]) -> dict[str, list[str]]:
        soup = BeautifulSoup(html, "html.parser")
        results: dict[str, list[str]] = {}
        for sel in selectors:
            elems = soup.select(sel)
            results[sel] = [el.get_text(strip=True) for el in elems]
        return results

    def extract_xpath(self, html: str, xpaths: list[str]) -> dict[str, list[str]]:
        if not HAS_LXML:
            raise RuntimeError("XPath extraction requires the 'lxml' package.")
        tree = lxml.html.fromstring(html)
        results: dict[str, list[str]] = {}
        for xp in xpaths:
            nodes = tree.xpath(xp)
            texts: list[str] = []
            for node in nodes:
                if isinstance(node, str):
                    texts.append(node.strip())
                elif hasattr(node, "text_content"):
                    texts.append(node.text_content().strip())
                else:
                    texts.append(str(node).strip())
            results[xp] = texts
        return results

    def scrape(self, url: str, selectors: list[str] | None = None, xpaths: list[str] | None = None) -> dict[str, list[str]]:
        html = self.fetch_page(url)
        if not html:
            return {}
        data: dict[str, list[str]] = {}
        if selectors:
            data.update(self.extract_css(html, selectors))
        if xpaths:
            data.update(self.extract_xpath(html, xpaths))
        return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Basic HTML scraper")
    parser.add_argument("--url", help="Target URL to scrape")
    parser.add_argument(
        "--urls-file",
        help="Fallback file containing URLs (one per line)"
    )
    parser.add_argument(
        "--selector",
        action="append",
        help="CSS selector (may be used multiple times)",
    )
    parser.add_argument(
        "--xpath",
        action="append",
        help="XPath expression (may be used multiple times)",
    )
    parser.add_argument("--output", help="File to save the extracted text")
    args = parser.parse_args()

    urls: list[str] = []
    if args.url:
        urls = [args.url]
    elif os.getenv("BASIC_HTML_URL"):
        urls = [os.getenv("BASIC_HTML_URL", "")]
    elif args.urls_file:
        try:
            with open(args.urls_file, "r", encoding="utf-8") as f:
                urls = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"{args.urls_file} not found.", file=sys.stderr)
    if not urls:
        parser.error("No target URL provided. Use --url or BASIC_HTML_URL.")

    scraper = BasicHTMLScraper()

    all_texts = []
    for url in urls:
        data = scraper.scrape(url, selectors=args.selector, xpaths=args.xpath)
        all_texts.extend(text for values in data.values() for text in values)

    plain_text = "\n".join(all_texts)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(plain_text)
    else:
        print(plain_text)


if __name__ == "__main__":
    main()
