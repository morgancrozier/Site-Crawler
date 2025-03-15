import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import time
from collections import deque
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging
from contextlib import contextmanager
from tqdm import tqdm
import re
import csv
import json
from datetime import datetime
import statistics
import yaml
import os
from typing import Dict, Any

# Initialize logger at module level
logger = logging.getLogger(__name__)

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def setup_logging(config: Dict[str, Any]) -> None:
    """Setup logging based on configuration."""
    log_config = config['logging']
    handlers = [logging.StreamHandler()]
    
    if log_config.get('file'):
        handlers.append(logging.FileHandler(log_config['file']))
    
    logging.basicConfig(
        level=getattr(logging, log_config['level']),
        format=log_config['format'],
        handlers=handlers
    )

class SeoCrawler:
    def __init__(self, config: Dict[str, Any]):
        """Initialize crawler with configuration dictionary."""
        self.config = config
        self.start_url = self.normalize_url(config['target']['start_url'])
        self.max_pages = config['target']['max_pages']
        self.delay = config['performance']['delay']
        self.max_workers = min(config['performance']['max_workers'], 10)
        self.timeout = config['performance']['timeout']
        self.batch_size = config['performance']['batch_size']
        self.headers = config['http']['headers']
        self.keyword = config['target']['keyword']
        self.max_headings_per_level = config['output']['max_headings_per_level']
        
        # Compile skip patterns
        self.skip_patterns = re.compile('|'.join(config['http']['skip_patterns']))
        
        self.visited = set()
        self.queue = deque([self.start_url])
        self.lock = threading.Lock()
        self.session = requests.Session()
        self.stats = {
            'response_times': [],
            'broken_links': [],
            'content_types': {},
            'status_codes': {},
            'start_time': None,
            'end_time': None
        }
        
        self.setup_db()

    @staticmethod
    def normalize_url(url):
        """Normalize URL to avoid duplicates."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"

    @contextmanager
    def database_connection(self):
        """Context manager for SQLite connection."""
        conn = sqlite3.connect(self.config['output']['database_file'], timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            conn.close()

    def setup_db(self):
        """Initialize or update SQLite database with multiple heading columns."""
        with self.database_connection() as conn:
            # Create table with base columns if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pages (
                    url TEXT PRIMARY KEY,
                    status INTEGER,
                    response_time REAL,
                    content_type TEXT,
                    title TEXT,
                    description TEXT,
                    canonical TEXT,
                    robots_meta TEXT,
                    word_count INTEGER,
                    images TEXT,
                    last_crawled TIMESTAMP,
                    error TEXT
                )
            """)
            # Add heading columns (h1_1, h1_2, ..., h6_5)
            for level in range(1, 7):  # H1 to H6
                for i in range(1, self.max_headings_per_level + 1):
                    column_name = f"h{level}_{i}"
                    try:
                        conn.execute(f"ALTER TABLE pages ADD COLUMN {column_name} TEXT")
                    except sqlite3.OperationalError:  # Column already exists
                        pass
            conn.commit()

    def can_fetch(self, url):
        """Check if URL is crawlable based on robots.txt."""
        rp = RobotFileParser()
        robots_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}/robots.txt"
        try:
            response = requests.get(robots_url, headers=self.headers, timeout=5)
            rp.parse(response.text.splitlines())
            return rp.can_fetch("*", url)
        except Exception as e:
            logger.warning(f"Failed to check robots.txt for {url}: {e}")
            return False  # Default to no crawl on failure

    def is_html_content(self, response):
        """Check if response is likely HTML."""
        content_type = response.headers.get("Content-Type", "").lower()
        return "text/html" in content_type

    def fetch_page(self, url):
        """Fetch and parse a single page."""
        if self.should_skip_url(url):
            return None

        start_time = time.time()
        try:
            response = self.session.get(url, headers=self.headers, timeout=self.timeout)
            response_time = time.time() - start_time
            
            self.stats['response_times'].append(response_time)
            self.stats['status_codes'][response.status_code] = self.stats['status_codes'].get(response.status_code, 0) + 1
            
            content_type = response.headers.get('Content-Type', '').lower()
            self.stats['content_types'][content_type] = self.stats['content_types'].get(content_type, 0) + 1

            if not self.is_html_content(response):
                return None

            soup = BeautifulSoup(response.text, 'lxml')  # Using lxml for better performance

            # Basic metadata
            title = soup.title.string.strip() if soup.title else "No Title"
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            description = meta_desc['content'].strip() if meta_desc and 'content' in meta_desc.attrs else "No Description"
            canonical_tag = soup.find('link', attrs={'rel': 'canonical'})
            canonical = canonical_tag['href'].strip() if canonical_tag and 'href' in canonical_tag.attrs else "No Canonical"
            
            # Robots meta
            robots_meta = soup.find('meta', attrs={'name': 'robots'})
            robots_content = robots_meta['content'] if robots_meta and 'content' in robots_meta.attrs else "Not specified"

            # Content analysis
            body_text = soup.get_text(strip=True)
            word_count = len(body_text.split())

            # Image analysis
            images = [
                {
                    'src': urljoin(url, img.get('src', '')),
                    'alt': img.get('alt', 'No Alt').strip(),
                    'title': img.get('title', ''),
                    'dimensions': f"{img.get('width', 'unknown')}x{img.get('height', 'unknown')}"
                }
                for img in soup.find_all('img')
            ]

            result = {
                'url': url,
                'status': response.status_code,
                'response_time': response_time,
                'content_type': content_type,
                'title': title,
                'description': description,
                'canonical': canonical,
                'robots_meta': robots_content,
                'word_count': word_count,
                'images': json.dumps(images),
                'last_crawled': datetime.now().isoformat(),
                'error': None,
                'links': []
            }

            # Extract headings
            for level in range(1, 7):
                headings = [tag.get_text().strip() for tag in soup.find_all(f'h{level}')]
                for i in range(1, self.max_headings_per_level + 1):
                    column_name = f"h{level}_{i}"
                    result[column_name] = headings[i-1] if i <= len(headings) else f"No H{level}-{i}"

            # Extract links
            result['links'] = [
                self.normalize_url(urljoin(url, link['href']))
                for link in soup.find_all('a', href=True)
                if urlparse(urljoin(url, link['href'])).netloc == urlparse(self.start_url).netloc
                and not self.should_skip_url(link['href'])
            ]

            return result

        except requests.Timeout:
            self.stats['broken_links'].append((url, "Timeout"))
            return self._create_error_result(url, "Timeout Error", start_time)
        except requests.RequestException as e:
            self.stats['broken_links'].append((url, str(e)))
            return self._create_error_result(url, str(e), start_time)
        except Exception as e:
            logger.error(f"Unexpected error processing {url}: {e}")
            return self._create_error_result(url, f"Unexpected Error: {str(e)}", start_time)

    def _create_error_result(self, url, error_message, start_time):
        """Create a result dictionary for failed requests."""
        return {
            'url': url,
            'status': 0,
            'response_time': time.time() - start_time,
            'content_type': 'error',
            'title': "Error",
            'description': "Error",
            'canonical': "Error",
            'robots_meta': "Error",
            'word_count': 0,
            'images': json.dumps([]),
            'last_crawled': datetime.now().isoformat(),
            'error': error_message,
            'links': []
        }

    def should_skip_url(self, url):
        """Check if URL should be skipped based on patterns."""
        return bool(self.skip_patterns.search(url))

    def save_results(self, results):
        """Batch save results to the database with multiple heading columns."""
        with self.database_connection() as conn:
            columns = ["url", "status", "title", "description", "canonical", "word_count", "images"]
            for level in range(1, 7):
                for i in range(1, self.max_headings_per_level + 1):
                    columns.append(f"h{level}_{i}")
            placeholders = ",".join("?" * len(columns))
            query = f"INSERT OR REPLACE INTO pages ({','.join(columns)}) VALUES ({placeholders})"
            conn.executemany(query, [
                tuple(r[col] for col in columns) for r in results
            ])
            conn.commit()

    def export_to_csv(self):
        """Export database contents to CSV file."""
        csv_file = self.config['output']['csv_file']
        with self.database_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pages")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
        
        logger.info(f"Exported results to {csv_file}")

    def export_to_json(self):
        """Export database contents to JSON file."""
        json_file = self.config['output']['json_file']
        with self.database_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pages")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            data = [dict(zip(columns, row)) for row in rows]
            
            with open(json_file, "w") as f:
                json.dump(data, f, indent=2)
        
        logger.info(f"Exported results to {json_file}")

    def generate_report(self):
        """Generate a summary report of the crawl."""
        try:
            start_time = datetime.fromisoformat(self.stats['start_time'])
            end_time = datetime.fromisoformat(self.stats['end_time'])
            duration = (end_time - start_time).total_seconds()
        except (TypeError, ValueError) as e:
            logger.error(f"Error calculating duration: {e}")
            duration = 0

        report = {
            'crawl_summary': {
                'start_time': self.stats['start_time'],
                'end_time': self.stats['end_time'],
                'duration_seconds': duration,
                'pages_crawled': len(self.visited),
                'avg_response_time': statistics.mean(self.stats['response_times']) if self.stats['response_times'] else 0,
                'median_response_time': statistics.median(self.stats['response_times']) if self.stats['response_times'] else 0,
                'status_codes': self.stats['status_codes'],
                'content_types': self.stats['content_types'],
                'broken_links_count': len(self.stats['broken_links'])
            },
            'broken_links': self.stats['broken_links'],
            'configuration': self.config
        }

        report_file = self.config['output']['report_file']
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Crawl report generated: {report_file}")

    def crawl(self):
        """Main crawl loop with progress bar and CSV export."""
        self.stats['start_time'] = datetime.now().isoformat()
        pages_crawled = 0
        results_buffer = []

        with tqdm(total=self.max_pages, desc="Crawling", unit="page") as pbar:
            while self.queue and len(self.visited) < self.max_pages:
                with self.lock:
                    batch_size = min(self.max_workers, len(self.queue), self.max_pages - len(self.visited))
                    batch = [self.queue.popleft() for _ in range(batch_size) if self.queue]

                urls_to_crawl = []
                for url in batch:
                    if url not in self.visited and self.can_fetch(url):
                        urls_to_crawl.append(url)
                    else:
                        with self.lock:
                            self.visited.add(url)

                if not urls_to_crawl:
                    continue

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_url = {executor.submit(self.fetch_page, url): url for url in urls_to_crawl}
                    for future in as_completed(future_to_url):
                        url = future_to_url[future]
                        try:
                            result = future.result()
                            if result:  # Only process if result is not None
                                results_buffer.append(result)
                                with self.lock:
                                    self.visited.add(url)
                                    pages_crawled += 1
                                    for link in result["links"]:
                                        if link not in self.visited and link not in self.queue:
                                            self.queue.append(link)
                                pbar.update(1)
                                logger.info(f"Crawled: {url} (Status: {result['status']}) - {pages_crawled}/{self.max_pages}")
                        except Exception as e:
                            logger.error(f"Error processing {url}: {e}")

                # Batch save every 50 results or at the end
                if len(results_buffer) >= self.batch_size or len(self.visited) >= self.max_pages:
                    self.save_results(results_buffer)
                    results_buffer.clear()

                time.sleep(self.delay)

        # Save any remaining results
        if results_buffer:
            self.save_results(results_buffer)

        # Set end time before generating reports
        self.stats['end_time'] = datetime.now().isoformat()

        # Export to CSV after crawling
        self.export_to_csv()

        # Generate and save report
        self.generate_report()

        logger.info(f"Finished crawling {pages_crawled} pages.")

    def cleanup(self):
        """Cleanup resources."""
        try:
            self.session.close()
            # Export any remaining data
            self.stats['end_time'] = datetime.now().isoformat()
            self.generate_report()
            self.export_to_csv()
            self.export_to_json()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    """Main function to run the crawler."""
    crawler = None
    try:
        # Load configuration
        config = load_config()
        
        # Setup logging
        setup_logging(config)
        
        # Initialize and run crawler
        crawler = SeoCrawler(config)
        
        # Verify initial URL is accessible
        initial_result = crawler.fetch_page(crawler.start_url)
        if not initial_result:
            raise Exception(f"Initial URL {crawler.start_url} is not accessible or not HTML content")
        
        crawler.crawl()
        
    except KeyboardInterrupt:
        logger.info("Crawling interrupted by user.")
        if crawler:
            logger.info("Performing cleanup...")
            crawler.cleanup()
    except Exception as e:
        logger.error(f"Crawling failed: {e}")
        if crawler:
            crawler.cleanup()
        raise
    else:
        if crawler:
            crawler.cleanup()

if __name__ == "__main__":
    main()