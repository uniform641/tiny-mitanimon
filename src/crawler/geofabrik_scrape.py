#!/usr/bin/env python3
"""
geofabrik_scrape.py

从 Geofabrik download index 页面递归抓取每个 subregion 的:
 - name         (例如 "Africa")
 - parent       (父名称，顶层为空字符串)
 - size_bytes   (以字节为单位，若未知为 0)
 - size_str     (原始显示字符串，如 "(6.9 GB)")
 - download_link (指向 <region>-latest.osm.pbf 的完整 URL)

递归规则：
 - 若该子区域的 .osm.pbf 大小 > 1 GiB，递归进入对应的子页面抓取其内部项
 - 特例：顶层页面的 "Central America" 无论大小也会被递归抓取

用法:
  python geofabrik_scrape.py [root_url]
默认 root_url = "https://download.geofabrik.de/index.html"
输出: geofabrik_regions.csv
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import re
import time

BASE_URL = "https://download.geofabrik.de/index.html"
OUTPUT_CSV = "geofabrik_regions.csv"

visited = set()

def parse_size(size_str: str) -> float:
    """将 '6.9 GB' 转成字节数"""
    if not size_str:
        return 0
    s = size_str.strip().replace("(", "").replace(")", "").replace("\xa0", " ").upper()
    m = re.search(r"([\d.]+)\s*([KMG])B", s)
    if not m:
        return 0
    val, unit = m.groups()
    val = float(val)
    return val * {"K": 1024, "M": 1024**2, "G": 1024**3}[unit]

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def parse_table(url: str, parent: str):
    print(f"🔍 Parsing {url}")
    soup = get_soup(url)

    tables = soup.find_all("table", id="subregions")
    valid_tables = [t for t in tables if t.find("tr", onmouseover=True)]
    if not valid_tables:
        return []

    records = []

    for table in valid_tables:
        for tr in table.find_all("tr", onmouseover=True):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue

            # ---- 子区域名称 ----
            name_tag = tds[0].find("a")
            if not name_tag:
                continue
            name = name_tag.text.strip()
            region_html = urljoin(url, name_tag["href"])

            # ---- 下载链接 ----
            pbf_tag = tds[1].find("a")
            download_link = urljoin(url, pbf_tag["href"]) if pbf_tag else ""

            # ---- 文件大小 ----
            size_text_raw = tds[2].get_text(strip=True)
            size_text = size_text_raw.replace("(", "").replace(")", "").replace("\xa0", " ").strip()
            size_bytes = parse_size(size_text)

            records.append({
                "name": name,
                "parent": parent,
                "size": size_text,
                "download_link": download_link
            })

            # ---- 递归条件 ----
            href_lower = name_tag["href"].lower()
            if (
                size_bytes > 1 * 1024**3 or  # 大于1GB
                "central-america" in href_lower
            ):
                abs_url = urljoin(url, name_tag["href"])
                if abs_url not in visited:
                    visited.add(abs_url)
                    time.sleep(0.5)
                    records += parse_table(abs_url, parent=name)

    return records

def main():
    all_data = parse_table(BASE_URL, parent="root")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "parent", "size", "download_link"])
        writer.writeheader()
        writer.writerows(all_data)

    print(f"\n✅ Done! Collected {len(all_data)} rows → {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
