import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import re
import time

class GeofabrikCrawler():

    BASE_URL = "https://download.geofabrik.de/index.html"

    def __init__(self, db_path: str = 'db/boundary.duckdb',
                 output_csv_path: str = 'geofabrik_regions.csv'):
        self.db_path = db_path
        self.output_csv_path = output_csv_path
        self.data = None

    def parse_size(self, size_str: str) -> float:
        if not size_str:
            return 0
        s = size_str.strip().replace("(", "").replace(")", "").replace("\xa0", " ").upper()
        m = re.search(r"([\d.]+)\s*([KMG])B", s)
        if not m:
            return 0
        val, unit = m.groups()
        val = float(val)
        return val * {"K": 1024, "M": 1024**2, "G": 1024**3}[unit]

    def get_soup(self) -> BeautifulSoup:
        r = requests.get(self.BASE_URL)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    def parse_table(self, url: str, parent: str):
        print(f"🔍 Parsing {url}")
        soup = self.get_soup()

        tables = soup.find_all("table", id="subregions")
        valid_tables = [t for t in tables if t.find("tr", onmouseover=True)]
        if not valid_tables:
            return []

        records = []
        visited = set()

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
                size_bytes = self.parse_size(size_text)

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
                        time.sleep(1)
                        records += self.parse_table(abs_url, parent=name)

        return records

    def retrive(self):
        self.data = self.parse_table(self.BASE_URL, parent="root")

        print(f"\n✅ Done! Collected {len(self.data)} rows")

        return self.data

    def export_to_csv(self):
        with open(self.output_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "parent", "size", "download_link"], quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(self.data)

        print(f"\n✅ Done! Collected {len(self.data)} rows → {self.output_csv_path}")
