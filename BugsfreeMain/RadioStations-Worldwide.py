import requests
import json
import os
import re
from urllib.parse import urlparse
from collections import defaultdict
from datetime import datetime
import pytz
import concurrent.futures
import threading
import logging
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class M3UCollector:
    def __init__(self, country="Worldwide", base_dir="Output", check_links=True): # Changed base_dir for clarity
        self.channels = defaultdict(list)
        self.default_logo = "https://buddytv.netlify.app/img/no-logo.png" # Generic logo, can be adapted if needed
        self.seen_urls = set()
        self.url_status_cache = {}
        self.country = country # Store country name
        self.output_dir = os.path.join(base_dir, country.replace(" ", "_")) # Use country for output dir
        self.lock = threading.Lock()
        self.check_links = check_links  # Toggle link checking
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_content(self, url):
        """Fetch content (M3U or HTML) with streaming."""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        try:
            with requests.get(url, stream=True, headers=headers, timeout=10) as response:
                response.raise_for_status()
                # Handle potential encoding issues more gracefully
                lines = []
                for line_bytes in response.iter_lines():
                    try:
                        lines.append(line_bytes.decode('utf-8'))
                    except UnicodeDecodeError:
                        lines.append(line_bytes.decode('latin-1', errors='ignore')) # Fallback encoding

                content = '\n'.join(lines)
                if not lines:
                    logging.warning(f"No content fetched from {url}")
                else:
                    logging.info(f"Fetched {len(lines)} lines from {url}")
                return content, lines
        except requests.RequestException as e:
            logging.error(f"Failed to fetch {url}: {str(e)}")
            return None, []

    def extract_stream_urls_from_html(self, html_content, base_url):
        """Extract streaming URLs from HTML (less relevant for direct M3U but kept for robustness)."""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        stream_urls = set()
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            parsed_base = urlparse(base_url)
            parsed_href = urlparse(href)
            if not parsed_href.scheme:
                href = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
            
            # Adjusted regex for broader stream format matching, including typical radio streams
            if (href.endswith(('.m3u', '.m3u8', '.pls', '.aac', '.mp3', '.ogg')) or
                re.match(r'^https?://.*\.(ts|mp4|avi|mkv|flv|wmv|aac|mp3|ogg)$', href) or
                'playlist' in href.lower() or 'stream' in href.lower() or 'listen' in href.lower()):
                if not any(exclude in href.lower() for exclude in ['telegram', '.html', '.php', 'github.com', 'login', 'signup']):
                    stream_urls.add(href)
        
        logging.info(f"Extracted {len(stream_urls)} streaming URLs from {base_url}")
        return list(stream_urls)

    def check_link_active(self, url, timeout=2):
        """Check if a link is active, optimized for speed."""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        with self.lock:
            if url in self.url_status_cache:
                return self.url_status_cache[url]
        
        try:
            response = requests.head(url, timeout=timeout, headers=headers, allow_redirects=True)
            if response.status_code < 400:
                logging.info(f"Checked {url}: Active (HEAD)")
                with self.lock:
                    self.url_status_cache[url] = (True, url)
                return True, url
        except requests.RequestException:
            try:
                with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
                    # Check if content type suggests a valid stream (optional, can be broad)
                    # content_type = r.headers.get('Content-Type', '').lower()
                    # is_valid_stream_type = 'audio' in content_type or 'mpegurl' in content_type or 'octet-stream' in content_type
                    if r.status_code < 400: # and is_valid_stream_type:
                        logging.info(f"Checked {url}: Active (GET)")
                        with self.lock:
                            self.url_status_cache[url] = (True, url)
                        return True, url
            except requests.RequestException as e:
                logging.warning(f"Link check failed for {url}: {e}")
                # Removed protocol switching for radio as it's less common and for speed
                with self.lock:
                    self.url_status_cache[url] = (False, url)
                return False, url
        
        # If HEAD failed and GET wasn't conclusive or also failed
        with self.lock:
            self.url_status_cache[url] = (False, url)
        return False, url


    def parse_and_store(self, lines, source_url):
        """Parse M3U lines and store channels (radio stations)."""
        current_channel = {}
        channel_count = 0
        for line in lines:
            line = line.strip()
            if line.startswith('#EXTINF:'):
                match_logo = re.search(r'tvg-logo="([^"]*)"', line) # Standard M3U logo
                logo = match_logo.group(1) if match_logo and match_logo.group(1) else self.default_logo
                
                # Also try to get artwork or radio-logo for radio specific M3Us
                if logo == self.default_logo:
                    match_art = re.search(r'radio-logo="([^"]*)"', line) or re.search(r'artUrl="([^"]*)"', line)
                    if match_art:
                        logo = match_art.group(1)

                match_group = re.search(r'group-title="([^"]*)"', line)
                group = match_group.group(1) if match_group else "Uncategorized"
                
                match_name = re.search(r',(.+)$', line)
                name = match_name.group(1).strip() if match_name else "Unnamed Station"
                # Clean up common radio name prefixes/suffixes if necessary
                name = re.sub(r'^\d+\.\s*\[[^\]]+\]\s*', '', name) # Remove "[geo-blocked]" prefixes etc.
                name = re.sub(r'\s*\(GEO-BLOCKED\)$', '', name, flags=re.IGNORECASE)


                current_channel = {
                    'name': name,
                    'logo': logo,
                    'group': group, # For radio, group is often genre or country
                    'source': source_url
                }
            elif line.startswith('http') and current_channel: # Ensure it's a URL and we have channel info
                # Basic validation for common radio stream extensions
                if not (line.lower().endswith(('.m3u', '.m3u8')) or any(ext in line.lower() for ext in ['.aac', '.mp3', '.ogg', '.opus', 'icecast', 'shoutcast'])):
                    # logging.debug(f"Skipping non-typical radio stream URL: {line} for channel {current_channel.get('name')}")
                    # current_channel = {} # Reset if URL is not suitable
                    # continue # Skip this URL if it doesn't look like a radio stream
                    pass # Keep it for now, link checker will verify

                with self.lock:
                    if line not in self.seen_urls:
                        self.seen_urls.add(line)
                        current_channel['url'] = line
                        self.channels[current_channel['group']].append(current_channel)
                        channel_count += 1
                current_channel = {}
        logging.info(f"Parsed {channel_count} potential stations from {source_url}")

    def filter_active_channels(self):
        """Filter out inactive channels, skippable for speed."""
        if not self.check_links:
            logging.info("Skipping link activity check as per configuration.")
            return
        
        active_channels = defaultdict(list)
        all_channels_to_check = []
        
        # Deduplicate URLs before checking
        urls_to_check_map = {} # url -> list of (group, channel_info)
        for group, chans in self.channels.items():
            for ch in chans:
                if ch['url'] not in urls_to_check_map:
                    urls_to_check_map[ch['url']] = []
                urls_to_check_map[ch['url']].append((group, ch))

        logging.info(f"Total unique URLs to check: {len(urls_to_check_map)}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor: # Increased workers slightly
            future_to_url_details = {
                executor.submit(self.check_link_active, url): (url, details_list)
                for url, details_list in urls_to_check_map.items()
            }
            for future in concurrent.futures.as_completed(future_to_url_details):
                url, original_details_list = future_to_url_details[future]
                try:
                    is_active, updated_url = future.result()
                    if is_active:
                        for group, channel in original_details_list:
                            # Create a new channel dict to avoid modifying shared one if URL was duplicated
                            active_channel_entry = channel.copy()
                            active_channel_entry['url'] = updated_url 
                            active_channels[group].append(active_channel_entry)
                except Exception as e:
                    logging.error(f"Error checking {url}: {e}")

        self.channels = active_channels
        logging.info(f"Active stations after filtering: {sum(len(ch_list) for ch_list in active_channels.values())}")


    def process_sources(self, source_urls):
        """Process sources sequentially."""
        self.channels.clear()
        self.seen_urls.clear()
        self.url_status_cache.clear()
        
        all_m3u_like_urls = set() # Includes .m3u, .m3u8 and other direct stream/playlist types
        for url in source_urls:
            content, lines = self.fetch_content(url)
            if url.lower().endswith(('.htm', '.html')): # If it's an HTML page
                extracted_urls = self.extract_stream_urls_from_html(content, url)
                all_m3u_like_urls.update(extracted_urls)
            elif lines: # Assumed to be an M3U or similar playlist format
                self.parse_and_store(lines, url)
            else:
                logging.warning(f"No lines to parse from source: {url}")

        # Process URLs extracted from HTML (if any)
        for m3u_url in all_m3u_like_urls:
            _, lines = self.fetch_content(m3u_url)
            if lines:
                self.parse_and_store(lines, m3u_url)
        
        if self.channels:
            self.filter_active_channels()
        else:
            logging.warning("No stations parsed from any source.")

    def export_m3u(self, filename="Radio_Stations.m3u"):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('#EXTM3U\n')
            for group, channels_list in sorted(self.channels.items()):
                for channel in sorted(channels_list, key=lambda x: x['name']):
                    f.write(f'#EXTINF:-1 tvg-logo="{channel["logo"]}" group-title="{group}",{channel["name"]}\n')
                    f.write(f'{channel["url"]}\n')
        logging.info(f"Exported M3U to {filepath}")
        return filepath

    def export_txt(self, filename="Radio_Stations.txt"):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for group, channels_list in sorted(self.channels.items()):
                f.write(f"Category: {group}\n") # Changed "Group" to "Category" for radio context
                for channel in sorted(channels_list, key=lambda x: x['name']):
                    f.write(f"  Name: {channel['name']}\n")
                    f.write(f"  URL: {channel['url']}\n")
                    f.write(f"  Logo: {channel['logo']}\n")
                    f.write(f"  Source M3U: {channel['source']}\n")
                    f.write("  " + "-" * 48 + "\n")
                f.write("\n")
        logging.info(f"Exported TXT to {filepath}")
        return filepath

    def export_json(self, filename="Radio_Stations.json"):
        filepath = os.path.join(self.output_dir, filename)
        # Use a timezone, e.g., UTC for neutrality, or a specific one like Asia/Kolkata if preferred
        tz = pytz.timezone('UTC') 
        current_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')
        
        # Sort channels within groups for consistent output
        sorted_channels = defaultdict(list)
        for group, channels_list in self.channels.items():
            sorted_channels[group] = sorted(channels_list, key=lambda x: x['name'])

        json_data = {
            "collection_title": f"{self.country} Radio Stations",
            "last_updated_utc": current_time,
            "total_stations": sum(len(ch_list) for ch_list in self.channels.values()),
            "categories": dict(sorted_channels) # Changed "channels" to "categories"
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        logging.info(f"Exported JSON to {filepath}")
        return filepath

    def export_custom(self, filename="Radio_Stations_Custom"):
        """Export to custom JSON format without extension."""
        filepath = os.path.join(self.output_dir, filename) # No .json extension by convention
        custom_data_list = []
        
        for group, channels_list in sorted(self.channels.items()):
            for channel in sorted(channels_list, key=lambda x: x['name']):
                custom_data_list.append({
                    "name": channel['name'],
                    "category": group, # Changed "type" to "category"
                    "stream_url": channel['url'], # Clarified field name
                    "logo_url": channel['logo']   # Clarified field name
                })
        
        # Structure the custom output slightly differently for better organization if needed
        # For now, a flat list of stations as in original example
        output_structure = {
             "collection_title": f"{self.country} Radio Stations (Custom Format)",
             "stations": custom_data_list
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_structure, f, ensure_ascii=False, indent=2)
        logging.info(f"Exported custom format to {filepath}")
        return filepath

def main():
    # Specific M3U source for Radio Worldwide
    source_urls = [
        "https://github.com/junguler/m3u-radio-music-playlists/raw/refs/heads/main/---everything-full-repo.m3u"
    ]
    
    country_name = "Radio Worldwide"
    # Set check_links=True for accuracy (takes longer), False for speed (includes all parsed links).
    # For a large radio list, check_links=True can be very time-consuming.
    # Consider check_links=False for initial runs or if speed is paramount.
    collector = M3UCollector(country=country_name, base_dir="Radio_Collections", check_links=False) 
    collector.process_sources(source_urls)
    
    # Define base filename from country name
    file_base_name = country_name.replace(" ", "_")

    # Export files
    collector.export_m3u(f"{file_base_name}.m3u")
    collector.export_txt(f"{file_base_name}.txt")
    collector.export_json(f"{file_base_name}.json")
    collector.export_custom(file_base_name) # Custom format often doesn't have an extension
    
    total_stations = sum(len(ch_list) for ch_list in collector.channels.values())
    
    # Using UTC for logging timestamp for neutrality
    utc_time = datetime.now(pytz.utc)
    logging.info(f"[{utc_time.strftime('%Y-%m-%d %H:%M:%S %Z')}] Collected {total_stations} unique stations for {country_name}")
    logging.info(f"Categories/Groups found: {len(collector.channels)}")
    logging.info(f"Output files are in: {collector.output_dir}")

if __name__ == "__main__":
    main()
