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

from __future__ import annotations

import certifi
import gzip
import logging
import os
import re
import sys
import tempfile

from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote
from xml.etree import ElementTree as ET

# root ca for *.samsungcloud.tv
ROOT_PEM = """-----BEGIN CERTIFICATE-----
MIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD
QTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB
CSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97
nh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt
43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P
T19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4
gdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO
BgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR
TLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw
DQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr
hMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg
06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF
PnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls
YSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk
CAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=
-----END CERTIFICATE-----"""

BUNDLE_PATH = "/tmp/epg-merge-filter-ca-bundle.pem"
with open(certifi.where()) as f:
    base = f.read()
with open(BUNDLE_PATH, "w") as f:
    f.write(base + "\n" + ROOT_PEM)

# curl_cffi: mimics real browser TLS/HTTP2 fingerprints to bypass Cloudflare
try:
    from curl_cffi import requests as cf_requests

    HAS_CURL_CFFI = True
except ImportError:
    try:
        import requests as cf_requests  # fallback; may be blocked by Cloudflare
    except ImportError:
        cf_requests = None
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

M3U_URL = os.environ.get("M3U_URL", "")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "output/merged_epg.xml.gz")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "6"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
CF_PROXY_URL = os.environ.get("CF_PROXY_URL", "").rstrip("/")
IMPERSONATE = os.environ.get("IMPERSONATE", "chrome")
RELEASE_LOG = os.environ.get("RELEASE_LOG", "output/release_notes.md")

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

    if cf_requests is None:
        raise RuntimeError("Install curl_cffi or requests to fetch remote EPG sources")

    if HAS_CURL_CFFI:
        resp = cf_requests.get(target, 
            headers={
                "User-Agent": "AptvPlayer/1.5.4",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            },
            impersonate=IMPERSONATE, **kwargs)
    else:
        log.warning(
            "curl_cffi not installed; falling back to requests (may be blocked by Cloudflare)"
        )
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


def fetch_to_file(url: str) -> Path:
    """Download a URL to a temporary file without buffering the body in memory."""
    target = _proxied(url)
    kwargs = dict(timeout=REQUEST_TIMEOUT)

    if cf_requests is None:
        raise RuntimeError("Install curl_cffi or requests to fetch remote EPG sources")

    headers = {
        "User-Agent": "AptvPlayer/1.5.4",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    if HAS_CURL_CFFI:
        resp = cf_requests.get(
            target, headers=headers, impersonate=IMPERSONATE, stream=True, **kwargs
        )
    else:
        log.warning(
            "curl_cffi not installed; falling back to requests (may be blocked by Cloudflare)"
        )
        resp = cf_requests.get(target, headers=headers, stream=True, **kwargs)

    resp.raise_for_status()

    with tempfile.NamedTemporaryFile(prefix="epg_", suffix=".xml", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        if hasattr(resp, "iter_content"):
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
        else:
            tmp.write(resp.content)

    return tmp_path


def fetch_text(url: str) -> str:
    data = fetch_bytes(url)
    try:
        return data.decode("utf-8").lstrip("\ufeff")
    except UnicodeDecodeError:
        return data.decode("latin-1")


# ---------------------------------------------------------------------------
# M3U parsing
# ---------------------------------------------------------------------------


def parse_m3u(text: str) -> tuple[list[str], set[str], list[tuple[str, str]]]:
    """
    Parse M3U text and return:
      epg_urls  : deduplicated, order-preserving list of EPG XML URLs
      tvg_ids   : set of all channel tvg-ids
      tvg_logos : list of (display_name, logo_url) for every entry that has a tvg-logo
    """
    epg_urls: list[str] = []
    tvg_ids: set[str] = set()
    seen_urls: set[str] = set()
    tvg_names: list[str] = []
    tvg_ids_duplicate: list[str] = []
    tvg_id_count = 0
    tvg_logos: list[tuple[str, str]] = []

    for line in text.splitlines():
        line = line.strip()

        # EPG URLs may appear in url-tvg or x-tvg-url attributes,
        # and may be comma-separated lists
        if line.startswith("##"):
            for attr in ("url-tvg", "x-tvg-url", "x-gary-epg-url"):
                m = re.search(rf'{attr}="([^"]*)"', line, re.IGNORECASE)
                if m:
                    for raw in m.group(1).split(","):
                        url = raw.strip()
                        if url and url not in seen_urls:
                            epg_urls.append(url)
                            seen_urls.add(url)

        m = re.search(r'tvg-id="([^"]*)"', line, re.IGNORECASE)
        if m:
            tvg_id_count = tvg_id_count + 1
            tid = m.group(1).strip()
            if tid:
                if tid in tvg_ids:
                    tvg_ids_duplicate.append(tid)
                else:
                    tvg_ids.add(tid)
            else:
                m = re.search(r'tvg-name="([^"]*)"', line, re.IGNORECASE)
                if m:
                    tname = m.group(1).strip()
                    if tname:
                        tvg_names.append(tname)

        logo_m = re.search(r'tvg-logo="([^"]*)"', line, re.IGNORECASE)
        if logo_m:
            logo_url = logo_m.group(1).strip()
            if logo_url:
                name_m = re.search(r'tvg-name="([^"]*)"', line, re.IGNORECASE)
                display_name = name_m.group(1).strip() if name_m else ""
                if not display_name:
                    id_m = re.search(r'tvg-id="([^"]*)"', line, re.IGNORECASE)
                    display_name = id_m.group(1).strip() if id_m else ""
                if not display_name:
                    display_name = line.rsplit(",", 1)[-1].strip()
                tvg_logos.append((display_name or logo_url, logo_url))

    channel_summary = [
        "## Channels",
        "### Summary",
        "| Total | Invalid | Duplicated | Unique |",
        "|--------|--------|--------|------:|",
        f"| {tvg_id_count} | {len(tvg_names)} | {len(tvg_ids_duplicate)} | {len(tvg_ids)} |",
    ]

    release_note("\n".join(channel_summary))

    if tvg_names:
        release_note(f"### Invalid IDs ({len(tvg_names)})")
        release_note("> Entries where `tvg-id` is empty or whitespace-only.")
        for tvg_name in sorted(tvg_names):
            release_note(f"- {tvg_name}")

    if tvg_ids_duplicate:
        release_note(f"### Duplicated IDs ({len(tvg_ids_duplicate)})")
        release_note(
            "> The same `tvg-id` appears on more than one M3U entry. First occurrence is kept."
        )
        for tvg_id in sorted(tvg_ids_duplicate):
            release_note(f"- {tvg_id}")

    log.info(
        f"Found {len(epg_urls)} EPG source(s), {tvg_id_count} tvg-id(s), {len(tvg_names)} invalid tvg-id(s), {len(tvg_ids_duplicate)} duplicated tvg-id(s), {len(tvg_ids)} unique tvg-id(s), {len(tvg_logos)} tvg-logo(s)"
    )

    return epg_urls, tvg_ids, tvg_logos


# ---------------------------------------------------------------------------
# tvg-logo availability check
# ---------------------------------------------------------------------------


def check_logo_url(url: str) -> tuple[bool, str]:
    target = _proxied(url)
    headers = {
        "User-Agent": "AptvPlayer/1.5.4",
        "Accept": "*/*",
    }
    resp = None
    try:
        if HAS_CURL_CFFI:
            resp = cf_requests.get(
                target,
                headers=headers,
                impersonate=IMPERSONATE,
                timeout=REQUEST_TIMEOUT,
                verify=BUNDLE_PATH,
            )
        else:
            resp = cf_requests.get(
                target, headers=headers, timeout=REQUEST_TIMEOUT, verify=BUNDLE_PATH,
            )
        status = resp.status_code
        return status < 400, str(status)
    except Exception as exc:
        return False, str(exc)
    finally:
        if resp is not None and hasattr(resp, "close"):
            resp.close()


def check_tvg_logos(tvg_logos: list[tuple[str, str]]) -> None:
    if not tvg_logos:
        return

    url_to_names: dict[str, list[str]] = {}
    for name, url in tvg_logos:
        url_to_names.setdefault(url, []).append(name)

    failed: list[tuple[str, list[str], str]] = []
    checked = 0

    log.info(f"Checking {len(url_to_names)} tvg-logo URL(s)")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(check_logo_url, url): url for url in url_to_names}
        for fut in as_completed(futures):
            url = futures[fut]
            ok, detail = fut.result()
            checked += 1
            if not ok:
                failed.append((url, url_to_names[url], detail))
                log.warning(f"  FAILED [{url}]: {detail}")

    log.info(f"Logo check complete: {checked - len(failed)}/{checked} OK")

    logo_summary = [
        "## Logo Check",
        f"### Summary ({checked - len(failed)}/{checked} OK)",
    ]
    release_note("\n".join(logo_summary))

    if failed:
        release_note(f"### Failed Logos ({len(failed)})")
        release_note("> Entries where tvg-logo is unavailable.")
        for url, names, detail in sorted(failed, key=lambda item: item[0]):
            names_str = ";".join(sorted(set(names)))
            release_note(f"- {names_str}: {url} ({detail})")


# ---------------------------------------------------------------------------
# EPG download & merge
# ---------------------------------------------------------------------------


@dataclass
class EpgDownloadResult:
    url: str
    root: ET.Element | None = None
    channels: int = 0
    programmes: int = 0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.root is not None


def is_gzip_file(path: Path) -> bool:
    with path.open("rb") as f:
        return f.read(2) == b"\x1f\x8b"


def parse_epg_file(path: Path, url: str, tvg_ids: set[str]) -> EpgDownloadResult:
    """Stream-parse one XMLTV file and keep only relevant channel/programme nodes."""
    filtered = ET.Element("tv")
    channels = 0
    programmes = 0
    source_root: ET.Element | None = None

    input_file = gzip.open(path, "rb") if is_gzip_file(path) else path.open("rb")
    with input_file as source:
        for event, elem in ET.iterparse(source, events=("start", "end")):
            if event == "start" and source_root is None:
                source_root = elem
                for attr, val in elem.attrib.items():
                    filtered.set(attr, val)
                continue

            if event != "end":
                continue

            if elem.tag == "channel":
                channels += 1
                cid = elem.get("id", "").strip()
                if cid in tvg_ids:
                    filtered.append(deepcopy(elem))
                elem.clear()
                if source_root is not None:
                    source_root.clear()
            elif elem.tag == "programme":
                programmes += 1
                channel = elem.get("channel", "").strip()
                if channel in tvg_ids:
                    filtered.append(deepcopy(elem))
                elem.clear()
                if source_root is not None:
                    source_root.clear()

    return EpgDownloadResult(url, filtered, channels, programmes)


def download_epg(url: str, tvg_ids: set[str]) -> EpgDownloadResult:
    """Download and stream-parse a single EPG XML."""
    tmp_path: Path | None = None
    try:
        log.info(f"Downloading EPG: {url}")
        tmp_path = fetch_to_file(url)
        result = parse_epg_file(tmp_path, url, tvg_ids)
        log.info(
            f"  OK {url} — channels: {result.channels}  programmes: {result.programmes}"
        )
        return result
    except Exception as exc:
        log.warning(f"  FAILED [{url}]: {exc}")
        return EpgDownloadResult(url, error=str(exc))
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


def write_epg_source_notes(results: list[EpgDownloadResult]) -> None:
    ok_count = sum(1 for result in results if result.ok)
    epg_source_summary = [
        "## EPG Sources",
        f"### Summary ({ok_count}/{len(results)} OK)",
        "| URL | Status | Channels | Programmes |",
        "|--------|--------|--------|------:|",
    ]
    release_note("\n".join(epg_source_summary))

    for result in results:
        if result.ok:
            release_note(
                f"| {result.url} | OK | {result.channels} | {result.programmes} |"
            )
        else:
            release_note(f"| {result.url} | {result.error} | - | - |")


def programme_key(prog: ET.Element) -> tuple[str, str, str] | tuple[str, str]:
    channel = prog.get("channel", "").strip()
    start = prog.get("start", "").strip()
    stop = prog.get("stop", "").strip()
    if channel and start:
        return channel, start, stop
    return channel, ET.tostring(prog, encoding="unicode")


class CountingWriter:
    def __init__(self, wrapped):
        self.wrapped = wrapped
        self.bytes_written = 0

    def write(self, data: bytes) -> int:
        self.bytes_written += len(data)
        return self.wrapped.write(data)

    def flush(self) -> None:
        self.wrapped.flush()


def merge_epg(roots: list[ET.Element], tvg_ids: set[str]) -> ET.Element:
    """
    Merge multiple <tv> root elements with deduplication and filtering:
      <channel id="X">        kept only when X is in tvg_ids
      <programme channel="X"> kept only when X is in tvg_ids
    """
    merged = ET.Element("tv")
    merged.set("generator-info-url", "https://github.com/iwinstar/epg_merge_filter")
    merged.set("generator-info-name", "epg-merge-filter")

    channel_elems: list[ET.Element] = []
    programme_elems: list[ET.Element] = []

    seen_channels: set[str] = set()
    seen_programmes: set[tuple[str, str, str] | tuple[str, str]] = set()
    programme_channels: set[str] = set()
    total_prog = 0
    duplicate_prog = 0

    for root in roots:
        # Carry over root-level attributes from the first source that defines them
        for attr, val in root.attrib.items():
            if attr not in merged.attrib:
                merged.set(attr, val)

        for ch in root.findall("channel"):
            cid = ch.get("id", "").strip()
            if cid in tvg_ids and cid not in seen_channels:
                channel_elems.append(ch)
                seen_channels.add(cid)

        for prog in root.findall("programme"):
            if prog.get("channel", "").strip() not in tvg_ids:
                continue

            key = programme_key(prog)
            if key in seen_programmes:
                duplicate_prog += 1
                continue

            programme_elems.append(prog)
            seen_programmes.add(key)
            programme_channels.add(prog.get("channel", "").strip())
            total_prog += 1

    for ch in channel_elems:
        merged.append(ch)
    for prog in programme_elems:
        merged.append(prog)

    log.info(
        f"Merge complete — channels kept: {len(seen_channels)}/{len(tvg_ids)}, "
        f"programme entries: {total_prog}, duplicate programmes skipped: {duplicate_prog}"
    )

    unmatched = tvg_ids - seen_channels
    channels_without_programmes = seen_channels - programme_channels

    merge_summary = [
        "## Merge Result",
        "### Summary",
        "| Channels | Unmatched | Matched | No Programme | Programmes | Duplicated Programmes |",
        "|--------|--------|--------|--------|--------|------:|",
        f"| {len(tvg_ids)} | {len(unmatched)} | {len(seen_channels)} | {len(channels_without_programmes)} | {total_prog} | {duplicate_prog} |",
    ]
    release_note("\n".join(merge_summary))

    if unmatched:
        release_note(f"### Unmatched IDs ({len(unmatched)})")
        release_note("> Entries where `tvg-id` not found in any EPG source")
        for uid in sorted(unmatched):
            release_note(f"- {uid}")

    if channels_without_programmes:
        release_note(
            f"### Channels Without Programmes ({len(channels_without_programmes)})"
        )
        release_note("> Matched `<channel>` entries that have no merged `<programme>`.")
        for cid in sorted(channels_without_programmes):
            release_note(f"- {cid}")

    return merged


# ---------------------------------------------------------------------------
# Icon URL upgrade
# ---------------------------------------------------------------------------

ICON_URL_REPLACEMENTS: list[tuple[str, str]] = [
    ("focus.telerama.fr/252x168", "focus.telerama.fr/1764x1176"),
    ("thumb.canalplus.pro/http/unsafe/256x143", "thumb.canalplus.pro/http/unsafe/1792x1001"),
    ("thumb.canalplus.pro/http/unsafe/640x360", "thumb.canalplus.pro/http/unsafe/1920x1080")
]


def upgrade_icon_urls(root: ET.Element) -> int:
    upgraded = 0
    for icon in root.findall(".//programme/icon"):
        src = icon.get("src", "")
        if not src:
            continue
        new_src = src
        for old, new in ICON_URL_REPLACEMENTS:
            new_src = new_src.replace(old, new)
        if new_src != src:
            icon.set("src", new_src)
            upgraded += 1
    return upgraded


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

    with gzip.open(path, "wb", compresslevel=9) as gz:
        counter = CountingWriter(gz)
        counter.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        counter.write(b'<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        counter.write(b'<!-- Generated with EPG Merge Filter -->\n')
        tree.write(counter, encoding="utf-8", xml_declaration=False)

    raw_kb = counter.bytes_written / 1024
    gz_kb = os.path.getsize(path) / 1024
    ratio = (1 - gz_kb / raw_kb) * 100 if raw_kb else 0
    log.info(
        f"Written to {path} ({gz_kb:.1f} KB compressed, {raw_kb:.1f} KB raw, {ratio:.0f}% reduction)"
    )

    file_path = Path(path)
    file_summary = [
        "### File Summary",
        "| Name | Raw Size | Compressed Size | Reduction |",
        "|--------|--------|--------|------:|",
        f"| {file_path.name} | {raw_kb:.1f} KB | {gz_kb:.1f} KB | {ratio:.0f}% |",
    ]
    release_note("\n".join(file_summary))


# ---------------------------------------------------------------------------
# Record release notes
# ---------------------------------------------------------------------------


def release_note(content: str, path: str = RELEASE_LOG):
    file_path = Path(path)
    if file_path.suffix != ".md":
        file_path = file_path.with_suffix(".md")

    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as f:
        f.write(content + "\n")


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
    epg_urls, tvg_ids, tvg_logos = parse_m3u(m3u_text)

    check_tvg_logos(tvg_logos)

    if not epg_urls:
        log.error("No EPG URLs found in M3U (url-tvg / x-tvg-url)")
        return 1

    results_by_url: dict[str, EpgDownloadResult] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_epg, url, tvg_ids): url for url in epg_urls}
        for fut in as_completed(futures):
            url = futures[fut]
            results_by_url[url] = fut.result()

    results = [results_by_url[url] for url in epg_urls]
    write_epg_source_notes(results)

    roots = [result.root for result in results if result.root is not None]

    if not roots:
        log.error("All EPG downloads failed")
        return 1

    merged = merge_epg(roots, tvg_ids)

    upgraded = upgrade_icon_urls(merged)
    log.info(f"Icon URLs upgraded: {upgraded}")

    write_xml(merged, OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())