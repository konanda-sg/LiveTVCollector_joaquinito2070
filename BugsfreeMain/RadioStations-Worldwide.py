import requests
import json
import os
import re
from urllib.parse import urlparse
from collections import defaultdict, deque
from datetime import datetime
import pytz
import concurrent.futures
import threading
import logging
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class M3UCollector:
    def __init__(self, country="Worldwide", base_dir="Output", check_links=True):
        self.channels = defaultdict(list)
        self.default_logo = "https://buddytv.netlify.app/img/no-logo.png"
        self.seen_urls = set() 
        self.url_status_cache = {}
        self.country = country
        self.output_dir = os.path.join(base_dir, country.replace(" ", "_"))
        self.lock = threading.Lock()
        self.check_links = check_links
        self.max_total_playlists_to_process = 500 
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_content(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        try:
            with requests.get(url, stream=True, headers=headers, timeout=10) as response:
                response.raise_for_status()
                lines = []
                for line_bytes in response.iter_lines():
                    try: lines.append(line_bytes.decode('utf-8'))
                    except UnicodeDecodeError: lines.append(line_bytes.decode('latin-1', errors='ignore'))
                content_string = '\n'.join(lines)
                if not lines: logging.warning(f"No content fetched from {url}")
                else: logging.info(f"Fetched {len(lines)} lines from {url}")
                return content_string, lines
        except requests.RequestException as e:
            logging.error(f"Failed to fetch {url}: {str(e)}")
            return None, []

    def extract_stream_urls_from_html(self, html_content, base_url):
        if not html_content: return []
        soup = BeautifulSoup(html_content, 'html.parser')
        stream_urls = set()
        for link_tag in soup.find_all(['a', 'iframe', 'source'], href=True) + soup.find_all('source', src=True):
            href = link_tag.get('href') or link_tag.get('src')
            if not href: continue
            href = href.strip()
            parsed_href = urlparse(href)
            if not parsed_href.scheme or not parsed_href.netloc:
                href = requests.compat.urljoin(base_url, href)
            
            path_lower = urlparse(href).path.lower() # Re-parse after potential urljoin
            # Added .ashx to recognized playlist/stream extensions
            if (path_lower.endswith(('.m3u', '.m3u8', '.pls', '.ashx')) or
                re.match(r'^https?://.*\.(ts|mp4|avi|mkv|flv|wmv|aac|mp3|ogg|opus)$', href, re.IGNORECASE) or
                any(keyword in href.lower() for keyword in ['playlist', 'stream', 'listen', 'play', 'hls']) or
                "tune.ashx" in path_lower): # Specific check for "tune.ashx" in path
                
                if not any(exclude in href.lower() for exclude in ['telegram', '.html', '.php', 'github.com/login', 'github.com/signup', 'accounts.google.com', 'facebook.com/login', 'javascript:']):
                    if re.match(r'^https?://', href):
                        stream_urls.add(href)
        
        logging.info(f"Extracted {len(stream_urls)} potential stream/playlist URLs from HTML at {base_url}")
        return list(stream_urls)

    def check_link_active(self, url, timeout=2):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        with self.lock:
            if url in self.url_status_cache: return self.url_status_cache[url]
        try:
            response = requests.head(url, timeout=timeout, headers=headers, allow_redirects=True)
            if response.status_code < 400:
                logging.info(f"Checked {url}: Active (HEAD)")
                with self.lock: self.url_status_cache[url] = (True, url)
                return True, url
        except requests.RequestException:
            try:
                with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
                    if r.status_code < 400:
                        logging.info(f"Checked {url}: Active (GET)")
                        with self.lock: self.url_status_cache[url] = (True, url)
                        return True, url
            except requests.RequestException as e: logging.warning(f"Link check failed for {url}: {e}")
        with self.lock: self.url_status_cache[url] = (False, url)
        return False, url

    def parse_and_store(self, lines, source_m3u_url):
        current_channel_info = {}
        channels_parsed_count = 0
        nested_playlists_to_requeue = [] # Renamed for clarity

        for line_content in lines:
            line_content = line_content.strip()
            if not line_content: continue

            if line_content.startswith('#EXTINF:'):
                match_logo = re.search(r'tvg-logo="([^"]*)"', line_content)
                logo = match_logo.group(1) if match_logo and match_logo.group(1) else self.default_logo
                if logo == self.default_logo:
                    art_match = re.search(r'radio-logo="([^"]*)"', line_content) or \
                                re.search(r'artUrl="([^"]*)"', line_content)
                    if art_match: logo = art_match.group(1)
                match_group = re.search(r'group-title="([^"]*)"', line_content)
                group = match_group.group(1) if match_group else "Uncategorized"
                match_name = re.search(r',(.+)$', line_content)
                name = match_name.group(1).strip() if match_name else "Unnamed Station"
                name = re.sub(r'^\d+\.\s*\[[^\]]+\]\s*', '', name) 
                name = re.sub(r'\s*\(GEO-BLOCKED\)$', '', name, flags=re.IGNORECASE)
                current_channel_info = {'name': name, 'logo': logo, 'group': group, 'source': source_m3u_url}
            
            elif line_content.startswith(('http://', 'https://', 'rtmp://', 'rtsp://')):
                is_playlist_to_requeue = False
                try:
                    path_lower = urlparse(line_content).path.lower()
                    # Playlists to re-queue: .m3u, .pls, .ashx (includes Tune.ashx if path ends with it)
                    # .m3u8 files are NOT re-queued from here; they are treated as stream URLs for the channel.
                    if path_lower.endswith(('.m3u', '.pls', '.ashx')):
                        is_playlist_to_requeue = True
                except Exception as e:
                    logging.warning(f"Could not parse URL for playlist check: {line_content} in {source_m3u_url} ({e})")
                    
                if is_playlist_to_requeue:
                    nested_playlists_to_requeue.append(line_content)
                    current_channel_info = {} 
                elif current_channel_info: 
                    with self.lock:
                        if line_content not in self.seen_urls:
                            self.seen_urls.add(line_content)
                            current_channel_info['url'] = line_content
                            self.channels[current_channel_info['group']].append(current_channel_info)
                            channels_parsed_count += 1
                    current_channel_info = {}

        if channels_parsed_count > 0 or len(nested_playlists_to_requeue) > 0:
            logging.info(f"From {source_m3u_url}: Parsed {channels_parsed_count} direct channels, found {len(nested_playlists_to_requeue)} nested playlists to re-queue.")
        return nested_playlists_to_requeue

    def process_sources(self, initial_source_urls):
        self.channels.clear(); self.seen_urls.clear(); self.url_status_cache.clear()
        processing_queue = deque()
        processed_or_queued_m3u_sources = set() 

        for url in initial_source_urls:
            if url not in processed_or_queued_m3u_sources:
                processing_queue.append(url)
                processed_or_queued_m3u_sources.add(url)
        
        playlists_processed_count = 0
        while processing_queue and playlists_processed_count < self.max_total_playlists_to_process:
            current_url = processing_queue.popleft()
            playlists_processed_count += 1
            logging.info(f"Processing URL ({playlists_processed_count}/{self.max_total_playlists_to_process}): {current_url} (Queue: {len(processing_queue)})")

            content_string, lines_list = self.fetch_content(current_url)
            if not lines_list and not content_string:
                logging.warning(f"No content from {current_url}, skipping."); continue

            is_parsable_playlist = False
            path_current_url_lower = urlparse(current_url).path.lower()

            if lines_list and lines_list[0].strip().upper() == "#EXTM3U":
                is_parsable_playlist = True # M3U or M3U8
            # .pls and .ashx are also considered directly parsable playlists
            elif path_current_url_lower.endswith(('.m3u', '.m3u8', '.pls', '.ashx')) and \
                 not (content_string and ("<html" in content_string.lower() or "<body" in content_string.lower())):
                is_parsable_playlist = True

            if is_parsable_playlist:
                logging.debug(f"Parsing {current_url} as playlist.")
                # parse_and_store will handle not re-queueing m3u8s from within,
                # but will return .m3u, .pls, .ashx found inside for re-queueing.
                nested_playlists = self.parse_and_store(lines_list, current_url)
                for nested_url in nested_playlists:
                    if nested_url not in processed_or_queued_m3u_sources:
                        logging.info(f"Queueing nested playlist: {nested_url} (from {current_url})")
                        processing_queue.append(nested_url)
                        processed_or_queued_m3u_sources.add(nested_url)
            elif (current_url.lower().endswith(('.html', '.htm'))) or \
                 (content_string and ("<html" in content_string.lower() or "<body" in content_string.lower())):
                logging.debug(f"Parsing {current_url} as HTML page.")
                extracted_links = self.extract_stream_urls_from_html(content_string, current_url)
                for link_url in extracted_links:
                    path_link_url_lower = urlparse(link_url).path.lower()
                    # All these types found on HTML should be queued for parsing.
                    # parse_and_store will then decide what to do with their contents.
                    if path_link_url_lower.endswith(('.m3u', '.m3u8', '.pls', '.ashx')):
                        if link_url not in processed_or_queued_m3u_sources:
                            logging.info(f"Queueing playlist from HTML: {link_url} (from {current_url})")
                            processing_queue.append(link_url)
                            processed_or_queued_m3u_sources.add(link_url)
            else: # Fallback for unknown content type, try to parse as a playlist
                logging.debug(f"Treating {current_url} as potential playlist (fallback).")
                nested_playlists = self.parse_and_store(lines_list, current_url)
                for nested_url in nested_playlists:
                    if nested_url not in processed_or_queued_m3u_sources:
                        logging.info(f"Queueing nested playlist (fallback): {nested_url} (from {current_url})")
                        processing_queue.append(nested_url)
                        processed_or_queued_m3u_sources.add(nested_url)
                # Log if nothing useful came from this fallback processing
                is_source_of_any_channel = any(ch['source'] == current_url for group in self.channels.values() for ch in group)
                if not nested_playlists and not is_source_of_any_channel:
                     logging.warning(f"No channels or re-queueable playlists added from fallback processing of {current_url}")

        logging.info(f"Finished processing sources. Total playlists attempted: {playlists_processed_count}.")
        if self.channels: self.filter_active_channels()
        else: logging.warning("No channels found after processing all sources.")

    def filter_active_channels(self):
        if not self.check_links:
            logging.info("Skipping link activity check as per configuration."); return
        active_channels = defaultdict(list); urls_to_check_map = {} 
        for group, chans in self.channels.items():
            for ch in chans:
                if ch['url'] not in urls_to_check_map: urls_to_check_map[ch['url']] = []
                urls_to_check_map[ch['url']].append((group, ch))
        logging.info(f"Total unique URLs to check for activity: {len(urls_to_check_map)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_url_details = { executor.submit(self.check_link_active, url): (url, d_list)
                for url, d_list in urls_to_check_map.items() }
            for future in concurrent.futures.as_completed(future_to_url_details):
                url, o_details_list = future_to_url_details[future]
                try:
                    is_active, updated_url = future.result()
                    if is_active:
                        for group, channel in o_details_list:
                            act_ch_entry = channel.copy(); act_ch_entry['url'] = updated_url 
                            active_channels[group].append(act_ch_entry)
                except Exception as e: logging.error(f"Error checking {url} during filtering: {e}")
        self.channels = active_channels
        logging.info(f"Active items after filtering: {sum(len(cl) for cl in active_channels.values())}")

    def export_m3u(self, filename="Radio_Stations.m3u"):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('#EXTM3U\n')
            for group, ch_list in sorted(self.channels.items()):
                for ch in sorted(ch_list, key=lambda x: x['name']):
                    f.write(f'#EXTINF:-1 tvg-logo="{ch["logo"]}" group-title="{group}",{ch["name"]}\n')
                    f.write(f'{ch["url"]}\n')
        logging.info(f"Exported M3U to {filepath}"); return filepath

    def export_txt(self, filename="Radio_Stations.txt"):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for group, ch_list in sorted(self.channels.items()):
                f.write(f"Category: {group}\n")
                for ch in sorted(ch_list, key=lambda x: x['name']):
                    f.write(f"  Name: {ch['name']}\n"); f.write(f"  URL: {ch['url']}\n")
                    f.write(f"  Logo: {ch['logo']}\n"); f.write(f"  Source M3U: {ch['source']}\n")
                    f.write("  " + "-" * 48 + "\n")
                f.write("\n")
        logging.info(f"Exported TXT to {filepath}"); return filepath

    def export_json(self, filename="Radio_Stations.json"):
        filepath = os.path.join(self.output_dir, filename)
        tz = pytz.timezone('UTC'); current_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')
        sorted_ch_data = defaultdict(list)
        for group, ch_list in self.channels.items():
            sorted_ch_data[group] = sorted(ch_list, key=lambda x: x['name'])
        json_data = { "collection_title": f"{self.country} Items", "last_updated_utc": current_time,
            "total_items": sum(len(cl) for cl in self.channels.values()), "categories": dict(sorted_ch_data) }
        with open(filepath, 'w', encoding='utf-8') as f: json.dump(json_data, f, ensure_ascii=False, indent=2)
        logging.info(f"Exported JSON to {filepath}"); return filepath

    def export_custom(self, filename="Radio_Stations_Custom"):
        filepath = os.path.join(self.output_dir, filename)
        custom_d_list = []
        for group, ch_list in sorted(self.channels.items()):
            for ch in sorted(ch_list, key=lambda x: x['name']):
                custom_d_list.append({ "name": ch['name'], "category": group,
                    "stream_url": ch['url'], "logo_url": ch['logo'] })
        output_struct = { "collection_title": f"{self.country} Items (Custom)", "items": custom_d_list }
        with open(filepath, 'w', encoding='utf-8') as f: json.dump(output_struct, f, ensure_ascii=False, indent=2)
        logging.info(f"Exported custom format to {filepath}"); return filepath

def main():
    source_urls = [
        "https://github.com/junguler/m3u-radio-music-playlists/raw/refs/heads/main/---everything-full-repo.m3u",
        # Add other M3U, PLS, ASHX URLs, or HTML pages linking to them
    ]
    country_name = "Radio Worldwide"
    collector = M3UCollector(country=country_name, base_dir="Radio_Collections", check_links=False) 
    collector.process_sources(source_urls)
    file_base_name = country_name.replace(" ", "_")
    collector.export_m3u(f"{file_base_name}.m3u")
    collector.export_txt(f"{file_base_name}.txt")
    collector.export_json(f"{file_base_name}.json")
    collector.export_custom(file_base_name)
    total_items = sum(len(cl) for cl in collector.channels.values())
    utc_time = datetime.now(pytz.utc)
    logging.info(f"[{utc_time.strftime('%Y-%m-%d %H:%M:%S %Z')}] Collected {total_items} unique items for {country_name}")
    logging.info(f"Categories/Groups found: {len(collector.channels)}")
    logging.info(f"Output files are in: {collector.output_dir}")

if __name__ == "__main__":
    main()
