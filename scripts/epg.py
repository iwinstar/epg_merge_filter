#!/usr/bin/env python3
"""
1. Parse all EPG source URLs (url-tvg / x-tvg-url) and channel tvg-ids from an M3U URL.
2. Download all EPG XMLs concurrently (supports gzip / deflate / plain).
3. Merge XMLs, keeping only <channel> and <programme> nodes whose ids match
   the tvg-ids found in the M3U.
4. Write the result to OUTPUT_PATH as a gzip-compressed file (default: output/merged_epg.xml.gz).

Environment variables:
    M3U_URL         Required. M3U playlist URL (Cloudflare-protected sites supported).
    OUTPUT_PATH     Optional. Output file path (default: output/merged_epg.xml.gz).
    MAX_WORKERS     Optional. Concurrent download threads (default: 6).
    REQUEST_TIMEOUT Optional. Per-request timeout in seconds (default: 30).
    CF_PROXY_URL    Optional. Cloudflare Workers reverse-proxy prefix.
                    Example: https://epg-proxy.your-name.workers.dev/proxy?url=
    IMPERSONATE     Optional. Browser fingerprint to impersonate (default: "chrome").
                    Use a bare alias to always track the latest fingerprint:
                      chrome        latest Chrome (recommended)
                      safari        latest Safari
                      safari_ios    latest Mobile Safari
                    Or pin to a specific version (must be supported by installed curl-cffi):
                      chrome136  chrome131  chrome124
                      edge146    safari17_0
"""

import gzip
import io
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from xml.etree import ElementTree as ET

# curl_cffi: mimics real browser TLS/HTTP2 fingerprints to bypass Cloudflare
try:
    from curl_cffi import requests as cf_requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests as cf_requests  # fallback; may be blocked by Cloudflare
    HAS_CURL_CFFI = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

M3U_URL         = os.environ.get("M3U_URL", "")
OUTPUT_PATH     = os.environ.get("OUTPUT_PATH", "output/merged_epg.xml.gz")
MAX_WORKERS     = int(os.environ.get("MAX_WORKERS", "6"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
CF_PROXY_URL    = os.environ.get("CF_PROXY_URL", "").rstrip("/")
IMPERSONATE     = os.environ.get("IMPERSONATE", "chrome")

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _proxied(url: str) -> str:
    """Wrap the target URL through the Workers reverse proxy if configured."""
    if CF_PROXY_URL:
        return f"{CF_PROXY_URL}?url={quote(url, safe='')}"
    return url


def fetch_bytes(url: str) -> bytes:
    """
    Download a URL and return raw (decompressed) bytes.
    Uses curl_cffi to mimic Chrome TLS + HTTP/2 fingerprints,
    bypassing Cloudflare Bot Management without a real browser.
    Falls back to plain requests if curl_cffi is unavailable.
    """
    target = _proxied(url)
    kwargs = dict(timeout=REQUEST_TIMEOUT)

    if HAS_CURL_CFFI:
        resp = cf_requests.get(target, impersonate=IMPERSONATE, **kwargs)
    else:
        log.warning("curl_cffi not installed; falling back to requests (may be blocked by Cloudflare)")
        resp = cf_requests.get(
            target,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Encoding": "gzip, deflate, br",
            },
            **kwargs,
        )

    resp.raise_for_status()
    data = resp.content
    # Some servers declare Content-Encoding but skip actual decompression
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data


def fetch_text(url: str) -> str:
    data = fetch_bytes(url)
    try:
        return data.decode("utf-8").lstrip("\ufeff")
    except UnicodeDecodeError:
        return data.decode("latin-1")


# ---------------------------------------------------------------------------
# M3U parsing
# ---------------------------------------------------------------------------

def parse_m3u(text: str) -> tuple[list[str], set[str]]:
    """
    Parse M3U text and return:
      epg_urls : deduplicated, order-preserving list of EPG XML URLs
      tvg_ids  : set of all channel tvg-ids
    """
    epg_urls: list[str] = []
    tvg_ids: set[str] = set()
    seen_urls: set[str] = set()

    for line in text.splitlines():
        line = line.strip()

        # EPG URLs may appear in url-tvg or x-tvg-url attributes,
        # and may be comma-separated lists
        for attr in ("url-tvg", "x-tvg-url"):
            m = re.search(rf'{attr}="([^"]*)"', line, re.IGNORECASE)
            if m:
                for raw in m.group(1).split(","):
                    url = raw.strip()
                    if url and url not in seen_urls:
                        epg_urls.append(url)
                        seen_urls.add(url)

        m = re.search(r'tvg-id="([^"]*)"', line, re.IGNORECASE)
        if m:
            tid = m.group(1).strip()
            if tid:
                tvg_ids.add(tid)
        else:
            m = re.search(r'tvg-name="([^"]*)"', line, re.IGNORECASE)
            if m:
                tid = m.group(1).strip()
                if tid:
                    tvg_ids.add(tid)

    return epg_urls, tvg_ids


# ---------------------------------------------------------------------------
# EPG download & merge
# ---------------------------------------------------------------------------

def download_epg(url: str) -> ET.Element | None:
    """Download and parse a single EPG XML; returns the root <tv> element."""
    try:
        log.info(f"Downloading EPG: {url}")
        data = fetch_bytes(url)
        root = ET.fromstring(data)
        log.info(
            f"  OK {url} — "
            f"channels: {len(root.findall('channel'))}  "
            f"programmes: {len(root.findall('programme'))}"
        )
        return root
    except Exception as exc:
        log.warning(f"  FAILED [{url}]: {exc}")
        return None


def merge_epg(roots: list[ET.Element], tvg_ids: set[str]) -> ET.Element:
    """
    Merge multiple <tv> root elements with deduplication and filtering:
      <channel id="X">        kept only when X is in tvg_ids
      <programme channel="X"> kept only when X is in tvg_ids
    """
    merged = ET.Element("tv")
    merged.set("generator-info-name", "epg-merger")

    seen_channels: set[str] = set()
    total_prog = 0

    for root in roots:
        # Carry over root-level attributes from the first source that defines them
        for attr, val in root.attrib.items():
            if attr not in merged.attrib:
                merged.set(attr, val)

        for ch in root.findall("channel"):
            cid = ch.get("id", "").strip()
            if cid in tvg_ids and cid not in seen_channels:
                merged.append(ch)
                seen_channels.add(cid)

        for prog in root.findall("programme"):
            if prog.get("channel", "").strip() in tvg_ids:
                merged.append(prog)
                total_prog += 1

    log.info(
        f"Merge complete — channels kept: {len(seen_channels)}/{len(tvg_ids)}, "
        f"programme entries: {total_prog}"
    )

    unmatched = tvg_ids - seen_channels
    if unmatched:
        log.warning(f"{len(unmatched)} tvg-id(s) not found in any EPG source:")
        for uid in sorted(unmatched):
            log.warning(f"  - {uid}")

    return merged


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_xml(root: ET.Element, path: str) -> None:
    """
    Serialize the merged XML tree and write it as a gzip-compressed file.
    Appends .gz to path automatically if not already present.
    """
    if not path.endswith(".gz"):
        path = path + ".gz"

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    ET.indent(root, space="  ")
    tree = ET.ElementTree(root)

    # Serialize to an in-memory buffer first, then compress to disk
    buf = io.BytesIO()
    buf.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
    buf.write(b'<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
    tree.write(buf, encoding="utf-8", xml_declaration=False)
    xml_bytes = buf.getvalue()

    with gzip.open(path, "wb", compresslevel=9) as gz:
        gz.write(xml_bytes)

    raw_kb = len(xml_bytes) / 1024
    gz_kb  = os.path.getsize(path) / 1024
    ratio  = (1 - gz_kb / raw_kb) * 100 if raw_kb else 0
    log.info(f"Written to {path} ({gz_kb:.1f} KB compressed, {raw_kb:.1f} KB raw, {ratio:.0f}% reduction)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    if not M3U_URL:
        log.error("M3U_URL environment variable is not set")
        return 1

    if not HAS_CURL_CFFI:
        log.warning("Install curl_cffi for Cloudflare bypass: pip install curl_cffi")

    log.info(f"Fetching M3U: {M3U_URL}")
    m3u_text = fetch_text(M3U_URL)
    epg_urls, tvg_ids = parse_m3u(m3u_text)
    log.info(f"Found {len(epg_urls)} EPG source(s), {len(tvg_ids)} unique tvg-id(s)")

    if not epg_urls:
        log.error("No EPG URLs found in M3U (url-tvg / x-tvg-url)")
        return 1

    roots: list[ET.Element] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_epg, url): url for url in epg_urls}
        for fut in as_completed(futures):
            r = fut.result()
            if r is not None:
                roots.append(r)

    if not roots:
        log.error("All EPG downloads failed")
        return 1

    merged = merge_epg(roots, tvg_ids)
    write_xml(merged, OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
