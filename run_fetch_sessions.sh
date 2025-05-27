#!/usr/bin/env bash


# Invoke local MCP server entrypoint directly
FETCH_CMD="node ./mcp_servers/github.com/zcaceres/fetch-mcp/dist/index.js"

# Install fetch-MCP globally if you haven’t already:
# npm install -g fetch-mcp

if [ ! -f ticketsibiza_event_data.json ]; then
  echo "ticketsibiza_event_data.json not found"
  exit 1
fi

mapfile -t EVENT_URLS < <(
  python3 - << 'PYCODE'
import json
data = json.load(open('ticketsibiza_event_data.json'))
urls = []
for rec in data:
    x = rec.get('answer') or rec.get('url') or []
    if isinstance(x, list):
        urls.extend(x)
    elif isinstance(x, str):
        urls.append(x)
print('\n'.join(urls))
PYCODE
)

mkdir -p output/json_no_llm output/md_with_llm

for i in {1..5}; do
  echo "=== Run $i: JSON (no LLM) via Python wrapper ==="
  for idx in "${!EVENT_URLS[@]}"; do
    url="${EVENT_URLS[idx]}"
    out="output/json_no_llm/run${i}_url${idx}.json"
    echo "  $url → $out"
    python3 - <<PYCODE > "$out" || echo "    [ERROR] $url"
import json
import sys
sys.path.insert(0, "")
from examples.smart_scraper_graph.ticketmaster_scraper import fetch_and_parse
data = fetch_and_parse("${url}", "JSON", False)
print(json.dumps(data))
PYCODE
  done

  echo "=== Run $i: Markdown (with LLM) via Python wrapper ==="
  for idx in "${!EVENT_URLS[@]}"; do
    url="${EVENT_URLS[idx]}"
    out="output/md_with_llm/run${i}_url${idx}.md"
    echo "  $url → $out"
    python3 - <<PYCODE > "$out" || echo "    [ERROR] $url"
from examples.smart_scraper_graph.ticketmaster_scraper import fetch_and_parse
result = fetch_and_parse("${url}", "Markdown", True)
print(result)
PYCODE
  done
done
