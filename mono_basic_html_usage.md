# mono_basic_html.py Usage Guide

This guide explains how to run the basic HTML scraper `mono_basic_html.py`.

## Installation

1. Install Python 3.8 or newer.
2. Install the dependencies listed in `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```
   The script relies on `requests`, `beautifulsoup4` and optionally `lxml` if you plan to use XPath.

## Running the Scraper

Invoke the script with a target URL and one or more CSS selectors or XPath expressions.
The URL can be supplied via the `--url` option or the `BASIC_HTML_URL` environment
variable. Optionally, you can provide `--urls-file` as a fallback list of URLs.

```bash
python mono_basic_html.py --url https://example.com --selector "h1" --selector ".content p"
```

To save the extracted text to a file, provide the `--output` argument:

```bash
python mono_basic_html.py --url https://example.com --selector "p" --output output.txt
```

If XPath extraction is requested, ensure `lxml` is installed:

```bash
python mono_basic_html.py --url https://example.com --xpath "//title" --xpath "//p[1]"
```

Without `--output`, the scraper prints the extracted text to standard output.
