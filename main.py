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
from datetime import datetime, timedelta
import statistics
import yaml
import os
from typing import Dict, Any, Optional
import psutil

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

class CrawlerStats:
    """Class to track and manage detailed crawler statistics."""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.last_stats_update = self.start_time
        self.pages_crawled = 0
        self.crawl_rates = []  # pages per minute
        self.memory_usage = []  # MB
        self.response_times = []
        self.errors = {
            'timeout': 0,
            'connection': 0,
            'http': 0,
            'parse': 0,
            'other': 0
        }
        self.content_types = {}
        self.status_codes = {}
        self.timing_stats = {
            'fetch': [],
            'parse': [],
            'save': []
        }

    def update_crawl_rate(self, pages_crawled: int, elapsed_seconds: float):
        """Update pages crawled per minute."""
        if elapsed_seconds > 0:
            rate = (pages_crawled * 60) / elapsed_seconds
            self.crawl_rates.append(rate)

    def update_memory_usage(self):
        """Update memory usage statistics."""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        self.memory_usage.append(memory_mb)

    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive statistics summary."""
        end_time = datetime.now()
        elapsed_time = (end_time - self.start_time).total_seconds()
        
        return {
            'timing': {
                'start_time': self.start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'elapsed_seconds': elapsed_time
            },
            'progress': {
                'pages_crawled': self.pages_crawled,
                'average_crawl_rate': statistics.mean(self.crawl_rates) if self.crawl_rates else 0,
                'current_crawl_rate': self.crawl_rates[-1] if self.crawl_rates else 0
            },
            'performance': {
                'memory_usage_mb': {
                    'current': self.memory_usage[-1] if self.memory_usage else 0,
                    'average': statistics.mean(self.memory_usage) if self.memory_usage else 0,
                    'peak': max(self.memory_usage) if self.memory_usage else 0
                },
                'response_times': {
                    'average': statistics.mean(self.response_times) if self.response_times else 0,
                    'median': statistics.median(self.response_times) if self.response_times else 0,
                    'p95': statistics.quantiles(self.response_times, n=20)[-1] if len(self.response_times) >= 20 else None
                }
            },
            'errors': self.errors,
            'content_types': self.content_types,
            'status_codes': self.status_codes,
            'timing_stats': {
                'fetch': {
                    'average': statistics.mean(self.timing_stats['fetch']) if self.timing_stats['fetch'] else 0,
                    'median': statistics.median(self.timing_stats['fetch']) if self.timing_stats['fetch'] else 0
                },
                'parse': {
                    'average': statistics.mean(self.timing_stats['parse']) if self.timing_stats['parse'] else 0,
                    'median': statistics.median(self.timing_stats['parse']) if self.timing_stats['parse'] else 0
                },
                'save': {
                    'average': statistics.mean(self.timing_stats['save']) if self.timing_stats['save'] else 0,
                    'median': statistics.median(self.timing_stats['save']) if self.timing_stats['save'] else 0
                }
            }
        }

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
        
        # Checkpoint configuration
        self.checkpoint_enabled = config['checkpoint']['enabled']
        self.checkpoint_interval = config['checkpoint']['interval']
        self.checkpoint_file = config['checkpoint']['file']
        self.keep_checkpoint = config['checkpoint']['keep_on_complete']
        
        # Progress tracking configuration
        self.progress_config = config['progress']
        self.stats_interval = config['progress']['stats_interval']
        
        # Initialize statistics
        self.stats = CrawlerStats()
        
        # Load or initialize state
        self.state = self.load_checkpoint() if self.checkpoint_enabled else None
        if self.state:
            self.visited = self.state['visited']
            self.queue = self.state['queue']
            self.stats.pages_crawled = len(self.visited)
        else:
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
            # Create main pages table
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

            # Create links table for internal link structure
            conn.execute("""
                CREATE TABLE IF NOT EXISTS links (
                    source_url TEXT,
                    target_url TEXT,
                    link_text TEXT,
                    is_followed BOOLEAN,
                    link_type TEXT,
                    discovered_at TIMESTAMP,
                    PRIMARY KEY (source_url, target_url),
                    FOREIGN KEY (source_url) REFERENCES pages(url),
                    FOREIGN KEY (target_url) REFERENCES pages(url)
                )
            """)
            
            # Add heading columns (h1_1, h1_2, ..., h6_5)
            for level in range(1, 7):
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

            soup = BeautifulSoup(response.text, 'lxml')

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
                'links': [],
                'links_data': []  # Enhanced link data
            }

            # Extract headings
            for level in range(1, 7):
                headings = [tag.get_text().strip() for tag in soup.find_all(f'h{level}')]
                for i in range(1, self.max_headings_per_level + 1):
                    column_name = f"h{level}_{i}"
                    result[column_name] = headings[i-1] if i <= len(headings) else f"No H{level}-{i}"

            # Enhanced link extraction
            for link in soup.find_all('a', href=True):
                target_url = self.normalize_url(urljoin(url, link['href']))
                # Only process internal links
                if urlparse(target_url).netloc == urlparse(self.start_url).netloc:
                    link_data = {
                        'url': target_url,
                        'text': link.get_text(strip=True),
                        'followed': not self.should_skip_url(target_url),
                        'type': 'internal'
                    }
                    if not self.should_skip_url(target_url):
                        result['links'].append(target_url)
                    result['links_data'].append(link_data)

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
            'links': [],
            'links_data': []
        }

    def should_skip_url(self, url):
        """Check if URL should be skipped based on patterns."""
        return bool(self.skip_patterns.search(url))

    def save_results(self, results):
        """Batch save results to the database."""
        with self.database_connection() as conn:
            # Save page data
            pages_columns = ["url", "status", "response_time", "content_type", "title", 
                           "description", "canonical", "robots_meta", "word_count", 
                           "images", "last_crawled", "error"]
            
            # Add heading columns
            for level in range(1, 7):
                for i in range(1, self.max_headings_per_level + 1):
                    pages_columns.append(f"h{level}_{i}")
            
            pages_placeholders = ",".join("?" * len(pages_columns))
            pages_query = f"INSERT OR REPLACE INTO pages ({','.join(pages_columns)}) VALUES ({pages_placeholders})"
            
            # Prepare links data
            links_data = []
            for result in results:
                # Extract page data
                page_data = [result[col] for col in pages_columns]
                conn.execute(pages_query, page_data)
                
                # Process links
                source_url = result['url']
                for link in result.get('links_data', []):
                    links_data.append((
                        source_url,
                        link['url'],
                        link['text'],
                        link['followed'],
                        link['type'],
                        datetime.now().isoformat()
                    ))
            
            # Batch insert links
            if links_data:
                conn.executemany("""
                    INSERT OR REPLACE INTO links 
                    (source_url, target_url, link_text, is_followed, link_type, discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, links_data)
            
            conn.commit()

    def export_to_csv(self):
        """Export database contents to CSV files."""
        # Export pages
        pages_file = self.config['output']['csv_file']
        links_file = pages_file.replace('.csv', '_links.csv')
        
        with self.database_connection() as conn:
            cursor = conn.cursor()
            
            # Export pages
            cursor.execute("SELECT * FROM pages")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            with open(pages_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            
            # Export links
            cursor.execute("SELECT * FROM links")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            with open(links_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
        
        logger.info(f"Exported pages to {pages_file}")
        logger.info(f"Exported links to {links_file}")

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

    def save_checkpoint(self):
        """Save current crawler state to checkpoint file."""
        if not self.checkpoint_enabled:
            return

        try:
            state = {
                'visited': list(self.visited),
                'queue': list(self.queue),
                'stats': self.stats.get_summary()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(state, f)
            logger.info(f"Checkpoint saved: {len(self.visited)} pages crawled")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    def update_stats(self):
        """Update detailed statistics."""
        if not self.progress_config['track_speed']:
            return

        now = datetime.now()
        elapsed = (now - self.stats.last_stats_update).total_seconds()
        
        if elapsed >= self.stats_interval:
            if self.progress_config['track_speed']:
                self.stats.update_crawl_rate(self.stats.pages_crawled, elapsed)
            
            if self.progress_config['track_memory']:
                self.stats.update_memory_usage()
            
            self.stats.last_stats_update = now
            
            # Save current statistics to file
            with open(self.config['output']['stats_file'], 'w') as f:
                json.dump(self.stats.get_summary(), f, indent=2)

    def crawl(self):
        """Main crawl loop with enhanced progress tracking and configurable checkpoints."""
        if not self.stats.start_time:
            self.stats.start_time = datetime.now()
        
        pages_since_checkpoint = 0
        
        with tqdm(total=self.max_pages, desc="Crawling", unit="page", 
                 initial=len(self.visited)) as pbar:
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
                                self.stats.pages_crawled += 1
                                pages_since_checkpoint += 1
                                
                                if self.progress_config['track_content_types']:
                                    content_type = result['content_type']
                                    self.stats.content_types[content_type] = \
                                        self.stats.content_types.get(content_type, 0) + 1
                                
                                if self.progress_config['track_status_codes']:
                                    status = result['status']
                                    self.stats.status_codes[status] = \
                                        self.stats.status_codes.get(status, 0) + 1
                                
                                if self.progress_config['track_timing']:
                                    self.stats.response_times.append(result['response_time'])

                                with self.lock:
                                    self.visited.add(url)
                                    for link in result["links"]:
                                        if link not in self.visited and link not in self.queue:
                                            self.queue.append(link)
                                pbar.update(1)
                                logger.info(f"Crawled: {url} (Status: {result['status']}) - {self.stats.pages_crawled}/{self.max_pages}")
                        except Exception as e:
                            logger.error(f"Error processing {url}: {e}")

                # Save results to database
                if urls_to_crawl:
                    self.save_results(urls_to_crawl)

                # Update statistics
                self.update_stats()

                # Save checkpoint if needed
                if self.checkpoint_enabled and pages_since_checkpoint >= self.checkpoint_interval:
                    self.save_checkpoint()
                    pages_since_checkpoint = 0

                time.sleep(self.delay)

        # Final cleanup and statistics
        self.cleanup()

    def cleanup(self):
        """Enhanced cleanup with final statistics and configurable checkpoint handling."""
        try:
            self.session.close()
            
            # Save final statistics
            self.stats.end_time = datetime.now()
            final_stats = self.stats.get_summary()
            
            with open(self.config['output']['stats_file'], 'w') as f:
                json.dump(final_stats, f, indent=2)
            
            # Save final checkpoint if enabled
            if self.checkpoint_enabled:
                self.save_checkpoint()
                
                # Remove checkpoint file if crawl completed successfully and keep_checkpoint is False
                if len(self.visited) >= self.max_pages and not self.keep_checkpoint:
                    try:
                        os.remove(self.checkpoint_file)
                        logger.info("Crawl completed successfully, checkpoint file removed")
                    except OSError:
                        pass
            
            # Generate reports and exports
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