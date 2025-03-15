import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import time
from collections import deque, defaultdict
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
from typing import Dict, Any, Optional, List, Set
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
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
    """Class to track and manage crawler statistics."""
    
    def __init__(self):
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.last_stats_update: datetime = datetime.now()
        self.pages_crawled: int = 0
        self.response_times: List[float] = []
        self.content_types: Dict[str, int] = defaultdict(int)
        self.status_codes: Dict[int, int] = defaultdict(int)
        self.errors: Dict[str, int] = defaultdict(int)
        self.memory_usage: List[float] = []
        self._last_speed_update = time.time()
        self._speed_window = []  # pages crawled in last minute
        self.broken_links: List[tuple] = []  # Store broken links (url, reason)
    
    def update_crawl_rate(self) -> float:
        """Calculate current crawl rate (pages/minute)."""
        current_time = time.time()
        self._speed_window = [t for t in self._speed_window if current_time - t <= 60]
        self._speed_window.append(current_time)
        return len(self._speed_window)
    
    def update_memory_usage(self) -> float:
        """Update memory usage statistics."""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        self.memory_usage.append(memory_mb)
        return memory_mb
    
    def add_broken_link(self, url: str, reason: str) -> None:
        """Add a broken link to the tracking list."""
        self.broken_links.append((url, reason))
    
    def restore_from_state(self, state: Dict[str, Any]) -> None:
        """Restore statistics from a saved state."""
        if not state:
            return
            
        stats = state.get('stats', {})
        self.pages_crawled = stats.get('pages_crawled', 0)
        
        if 'start_time' in stats and stats['start_time']:
            self.start_time = datetime.fromisoformat(stats['start_time'])
        
        self.response_times = stats.get('response_times', [])
        self.content_types = defaultdict(int, stats.get('content_types', {}))
        self.status_codes = defaultdict(int, stats.get('status_codes', {}))
        self.errors = defaultdict(int, stats.get('errors', {}))
        self.broken_links = stats.get('broken_links', [])
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive statistics summary."""
        duration = None
        if self.start_time:
            end = self.end_time or datetime.now()
            duration = (end - self.start_time).total_seconds()
        
        avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else 0
        avg_memory_usage = sum(self.memory_usage) / len(self.memory_usage) if self.memory_usage else 0
        
        return {
            'pages_crawled': self.pages_crawled,
            'duration_seconds': duration,
            'average_speed': self.pages_crawled / duration if duration else 0,
            'current_speed': self.update_crawl_rate(),
            'response_times': {
                'average': avg_response_time,
                'min': min(self.response_times) if self.response_times else 0,
                'max': max(self.response_times) if self.response_times else 0
            },
            'memory_usage': {
                'current': self.memory_usage[-1] if self.memory_usage else 0,
                'average': avg_memory_usage,
                'peak': max(self.memory_usage) if self.memory_usage else 0
            },
            'content_types': dict(self.content_types),
            'status_codes': {str(k): v for k, v in self.status_codes.items()},
            'errors': dict(self.errors),
            'broken_links': self.broken_links
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
            self.visited = set(self.state['visited'])
            self.queue = deque(self.state['queue'])
            self.stats.restore_from_state(self.state)
            logger.info(f"Restored state: {len(self.visited)} pages previously crawled")
        else:
            self.visited = set()
            self.queue = deque([self.start_url])
        
        self.lock = threading.Lock()
        self.session = requests.Session()
        
        self.setup_db()

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load crawler state from checkpoint file if it exists."""
        if not self.checkpoint_enabled or not os.path.exists(self.checkpoint_file):
            return None
        
        try:
            # First try reading the main checkpoint file
            with open(self.checkpoint_file, 'r') as f:
                state = json.load(f)
            
            # Validate checkpoint data
            required_keys = ['visited', 'queue', 'timestamp', 'crawl_status', 'stats']
            if not all(key in state for key in required_keys):
                raise ValueError("Checkpoint file is missing required data")
            
            # Check if checkpoint is too old
            checkpoint_time = datetime.fromisoformat(state['timestamp'])
            if (datetime.now() - checkpoint_time) > timedelta(hours=24):
                logger.warning("Checkpoint is more than 24 hours old, consider starting fresh")
            
            logger.info(f"Loaded checkpoint from {state['timestamp']}")
            logger.info(f"Crawl status: {state['crawl_status']['completion_percentage']:.1f}% complete")
            logger.info(f"Queue size: {state['crawl_status']['remaining_urls']} URLs")
            
            return state
            
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error loading checkpoint: {e}")
            
            # Try loading backup if main file is corrupted
            backup_file = f"{self.checkpoint_file}.bak"
            if os.path.exists(backup_file):
                try:
                    with open(backup_file, 'r') as f:
                        state = json.load(f)
                    logger.info("Successfully loaded checkpoint from backup file")
                    return state
                except Exception as backup_error:
                    logger.error(f"Failed to load checkpoint backup: {backup_error}")
            
            return None

    def save_checkpoint(self) -> None:
        """Save current crawler state to checkpoint file."""
        if not self.checkpoint_enabled:
            return

        state = {
            'visited': list(self.visited),
            'queue': list(self.queue),
            'timestamp': datetime.now().isoformat(),
            'crawl_status': {
                'in_progress': True,
                'last_url': list(self.visited)[-1] if self.visited else None,
                'remaining_urls': len(self.queue),
                'completion_percentage': (len(self.visited) / self.max_pages * 100) if self.max_pages else 0
            },
            'stats': {
                'pages_crawled': self.stats.pages_crawled,
                'start_time': self.stats.start_time.isoformat() if self.stats.start_time else None,
                'response_times': self.stats.response_times,
                'content_types': dict(self.stats.content_types),
                'status_codes': dict(self.stats.status_codes),
                'errors': dict(self.stats.errors),
                'broken_links': self.stats.broken_links,
                'memory_usage': {
                    'current': self.stats.memory_usage[-1] if self.stats.memory_usage else 0,
                    'peak': max(self.stats.memory_usage) if self.stats.memory_usage else 0
                }
            }
        }
        
        # Create backup of previous checkpoint
        if os.path.exists(self.checkpoint_file):
            backup_file = f"{self.checkpoint_file}.bak"
            try:
                os.replace(self.checkpoint_file, backup_file)
            except Exception as e:
                logger.warning(f"Failed to create checkpoint backup: {e}")
        
        try:
            # Write checkpoint atomically using temporary file
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            os.replace(temp_file, self.checkpoint_file)
            logger.info(f"Saved checkpoint: {len(self.visited)} pages crawled, {len(self.queue)} pages in queue")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
            # Try to restore backup if available
            if os.path.exists(f"{self.checkpoint_file}.bak"):
                try:
                    os.replace(f"{self.checkpoint_file}.bak", self.checkpoint_file)
                except Exception as backup_error:
                    logger.error(f"Failed to restore checkpoint backup: {backup_error}")

    def process_results(self, results: List[Dict[str, Any]]) -> None:
        """Process crawled page results and update statistics."""
        for result in results:
            url = result['url']
            
            # Update basic statistics
            self.stats.pages_crawled += 1
            
            # Update response time statistics
            if 'response_time' in result:
                self.stats.response_times.append(result['response_time'])
            
            # Update content type statistics
            if self.progress_config['track_content_types'] and 'content_type' in result:
                self.stats.content_types[result['content_type']] += 1
            
            # Update status code statistics
            if self.progress_config['track_status_codes'] and 'status' in result:
                self.stats.status_codes[result['status']] += 1
            
            # Process discovered links
            with self.lock:
                self.visited.add(url)
                for link in result.get('links', []):
                    if link not in self.visited and link not in self.queue:
                        self.queue.append(link)
            
            logger.info(f"Processed: {url} (Status: {result.get('status', 'unknown')}) - {self.stats.pages_crawled}/{self.max_pages}")

    def cleanup(self) -> None:
        """Perform final cleanup tasks and save statistics."""
        try:
            # Close resources
            self.session.close()
            
            # Save final statistics
            self.stats.end_time = datetime.now()
            final_stats = self.stats.get_summary()
            
            # Save statistics to file
            stats_file = self.config['output']['stats_file']
            with open(stats_file, 'w') as f:
                json.dump(final_stats, f, indent=2)
            logger.info(f"Final statistics saved to {stats_file}")
            
            # Handle checkpoint file
            if self.checkpoint_enabled:
                if len(self.visited) >= self.max_pages and not self.keep_checkpoint:
                    try:
                        os.remove(self.checkpoint_file)
                        logger.info("Checkpoint file removed after successful completion")
                    except OSError as e:
                        logger.error(f"Error removing checkpoint file: {e}")
                else:
                    self.save_checkpoint()
            
            # Generate final reports
            self.generate_report()
            self.export_to_csv()
            self.export_to_json()
            
            logger.info("Cleanup completed successfully")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise

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
            
            # Update statistics
            if self.progress_config['track_timing']:
                self.stats.response_times.append(response_time)
            
            if self.progress_config['track_status_codes']:
                self.stats.status_codes[response.status_code] = \
                    self.stats.status_codes.get(response.status_code, 0) + 1
            
            content_type = response.headers.get('Content-Type', '').lower()
            if self.progress_config['track_content_types']:
                self.stats.content_types[content_type] = \
                    self.stats.content_types.get(content_type, 0) + 1

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
            if self.progress_config['track_errors']:
                self.stats.errors['timeout'] = self.stats.errors.get('timeout', 0) + 1
            return self._create_error_result(url, "Timeout Error", start_time)
        except requests.RequestException as e:
            if self.progress_config['track_errors']:
                self.stats.errors['connection'] = self.stats.errors.get('connection', 0) + 1
            return self._create_error_result(url, str(e), start_time)
        except Exception as e:
            if self.progress_config['track_errors']:
                self.stats.errors['other'] = self.stats.errors.get('other', 0) + 1
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

    def save_results(self, results: Optional[List[Dict[str, Any]]] = None) -> None:
        """Batch save results to the database."""
        if not results:
            return
            
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
            stats_summary = self.stats.get_summary()
            
            report = {
                'crawl_summary': {
                    'start_time': self.stats.start_time.isoformat() if self.stats.start_time else None,
                    'end_time': self.stats.end_time.isoformat() if self.stats.end_time else None,
                    'duration_seconds': stats_summary['duration_seconds'],
                    'pages_crawled': stats_summary['pages_crawled'],
                    'avg_response_time': stats_summary['response_times']['average'],
                    'status_codes': stats_summary['status_codes'],
                    'content_types': stats_summary['content_types'],
                    'error_count': sum(stats_summary['errors'].values())
                },
                'performance': {
                    'memory_usage': stats_summary['memory_usage'],
                    'crawl_speed': {
                        'average': stats_summary['average_speed'],
                        'current': stats_summary['current_speed']
                    }
                },
                'errors': stats_summary['errors'],
                'configuration': self.config
            }

            report_file = self.config['output']['report_file']
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Crawl report generated: {report_file}")
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")

    def update_stats(self):
        """Update detailed statistics."""
        if not self.progress_config['track_speed']:
            return

        now = datetime.now()
        elapsed = (now - self.stats.last_stats_update).total_seconds()
        
        if elapsed >= self.stats_interval:
            if self.progress_config['track_speed']:
                self.stats.update_crawl_rate()
            
            if self.progress_config['track_memory']:
                self.stats.update_memory_usage()
            
            self.stats.last_stats_update = now
            
            # Save current statistics to file
            with open(self.config['output']['stats_file'], 'w') as f:
                json.dump(self.stats.get_summary(), f, indent=2)

    def crawl(self):
        """Start the crawling process."""
        logger.info(f"Starting crawl from {self.start_url}")
        if not self.stats.start_time:
            self.stats.start_time = datetime.now()
        last_checkpoint = time.time()
        last_stats_update = time.time()
        
        # Initialize progress bar
        progress = tqdm(
            total=self.max_pages,
            initial=len(self.visited),
            desc="Crawling",
            unit="pages"
        )

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                while self.queue and len(self.visited) < self.max_pages:
                    current_time = time.time()
                    
                    # Save checkpoint if interval elapsed
                    if self.checkpoint_enabled and (current_time - last_checkpoint) >= (self.checkpoint_interval * self.delay):
                        self.save_checkpoint()
                        last_checkpoint = current_time
                    
                    # Update statistics if interval elapsed
                    if current_time - last_stats_update >= self.stats_interval:
                        self.update_stats()
                        last_stats_update = current_time
                    
                    # Get next batch of URLs to process
                    batch_size = min(self.batch_size, self.max_pages - len(self.visited))
                    urls_to_process = []
                    while len(urls_to_process) < batch_size and self.queue:
                        url = self.queue.popleft()
                        if url not in self.visited:
                            urls_to_process.append(url)
                    
                    if not urls_to_process:
                        continue
                    
                    # Process batch of URLs
                    futures = [executor.submit(self.fetch_page, url) for url in urls_to_process]
                    results = []
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                results.append(result)
                                progress.update(1)
                        except Exception as e:
                            logger.error(f"Error processing future: {e}")
                    
                    if results:
                        self.process_results(results)
                        self.save_results(results)
                    
                    time.sleep(self.delay)  # Respect crawl delay

        except KeyboardInterrupt:
            logger.info("Crawl interrupted by user")
            self.save_checkpoint()  # Save checkpoint on interrupt
            raise
        except Exception as e:
            logger.error(f"Crawl failed: {e}")
            self.save_checkpoint()  # Save checkpoint on error
            raise
        finally:
            progress.close()
            self.stats.end_time = datetime.now()
            self.update_stats()
            self.generate_report()
            
            # Clean up checkpoint file if crawl completed successfully
            if self.checkpoint_enabled and not self.keep_checkpoint and len(self.visited) >= self.max_pages:
                try:
                    os.remove(self.checkpoint_file)
                    logger.info("Checkpoint file removed after successful completion")
                except Exception as e:
                    logger.error(f"Error removing checkpoint file: {e}")

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