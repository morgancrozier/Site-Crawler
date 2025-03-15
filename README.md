# SEO Crawler

A powerful and efficient web crawler designed for SEO analysis. This tool crawls websites while respecting robots.txt rules and collects comprehensive SEO-related data including titles, meta descriptions, headings (H1-H6), word counts, image information, internal links, and performance metrics.

## Features

- 🚀 Multi-threaded crawling with connection pooling
- 🤖 Respects robots.txt directives
- 📊 Comprehensive SEO data collection:
  - Page titles and meta descriptions
  - Canonical URLs
  - Robots meta tags
  - Multiple heading levels (H1-H6)
  - Word count analysis
  - Image data (src, alt text, dimensions, title)
  - Internal link detection and analysis
  - Response times and performance metrics
  - Content type detection
- 💾 Multiple output formats:
  - SQLite database with WAL mode
  - CSV export
  - JSON export
  - Detailed crawl report
- 📈 Performance monitoring:
  - Response time tracking
  - Status code distribution
  - Content type analysis
  - Broken link detection
  - Internal link structure
- ⚡ Enhanced error handling and recovery
- 🔒 Thread-safe operations
- ⚙️ YAML-based configuration
- 💪 Resilient crawling:
  - Automatic checkpointing
  - Crawl state persistence
  - Interrupt recovery
  - Progress restoration

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/seo-crawler.git
cd seo-crawler
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Configuration

The crawler is configured using `config.yaml`. Here are the main configuration sections:

### Target Configuration
```yaml
target:
  start_url: "https://example.com"
  max_pages: 100
  keyword: null  # Optional keyword for analysis
```

### Performance Settings
```yaml
performance:
  delay: 0.5  # Delay between requests in seconds
  max_workers: 5  # Maximum number of concurrent threads
  timeout: 30  # Request timeout in seconds
  batch_size: 50  # Database batch size
```

### HTTP Settings
```yaml
http:
  headers:
    User-Agent: "Your User Agent"
    Accept: "text/html,application/xhtml+xml..."
  skip_patterns:  # URLs matching these patterns will be skipped
    - "\\.css$"
    - "\\.js$"
    - "\\.jpg$"
    # ... more patterns
```

### Output Settings
```yaml
output:
  database_file: "crawl_results.db"
  csv_file: "crawl_results.csv"
  json_file: "crawl_results.json"
  report_file: "crawl_report.json"
  max_headings_per_level: 5
```

### Logging Configuration
```yaml
logging:
  level: "INFO"
  format: "%(asctime)s [%(levelname)s] %(message)s"
  file: "crawler.log"  # Optional file logging
```

## Usage

1. Basic usage:
```python
python main.py
```

The crawler will:
- Read configuration from `config.yaml`
- Load previous checkpoint (if exists)
- Start/resume crawling from the specified URL
- Save checkpoints periodically
- Save results to SQLite database
- Export results to CSV and JSON
- Generate a detailed crawl report

### Interruption & Recovery

The crawler implements automatic checkpointing to handle interruptions:

- Checkpoints are saved every 100 pages (configurable)
- State is preserved in `crawler_checkpoint.json`
- Includes:
  - Visited URLs
  - Remaining URL queue
  - Crawl statistics
  - Progress information

If the crawl is interrupted (Ctrl+C or error):
1. Current state is automatically saved
2. Running the crawler again will resume from last checkpoint
3. Progress is preserved
4. No duplicate crawling

The checkpoint file is automatically removed when:
- Crawl completes successfully
- Maximum page limit is reached

## Output Files

- `crawl_results.db`: SQLite database with all crawled data
- `crawl_results.csv`: CSV export of crawled pages data
- `crawl_results_links.csv`: CSV export of internal link structure
- `crawl_results.json`: JSON export of crawled data
- `crawl_report.json`: Detailed crawl statistics and analysis
- `crawler.log`: Logging output (if configured)
- `crawler_checkpoint.json`: Crawl state for recovery (temporary)

## Link Analysis

The crawler automatically identifies and analyzes internal links by:
- Detecting all `<a href>` tags in each page
- Normalizing URLs (removing trailing slashes, handling relative paths)
- Filtering for links within the same domain as the start URL
- Excluding non-HTML resource links (images, PDFs, etc.)
- Tracking link relationships for site structure analysis
- Storing link text and metadata
- Tracking whether links are followed or skipped

This data can be used to:
- Map internal link structure
- Identify orphaned pages
- Analyze site hierarchy
- Find broken internal links
- Generate site crawl paths
- Analyze anchor text distribution
- Identify navigation patterns

## Database Schema

The SQLite database includes two main tables:

### Pages Table
- Basic SEO data (title, description, canonical)
- Performance metrics (response time, status)
- Content analysis (word count, headings)
- Image data (with dimensions and accessibility info)
- Error tracking
- Timestamp information

### Links Table
- Source URL (the page containing the link)
- Target URL (the page being linked to)
- Link text (anchor text)
- Whether the link is followed
- Link type (internal/navigation/etc.)
- Discovery timestamp

This two-table structure allows for:
- Complete link relationship mapping
- Anchor text analysis
- Link path tracking
- Site structure visualization
- Navigation pattern analysis

## Requirements

- Python 3.6+
- BeautifulSoup4
- Requests
- tqdm
- PyYAML
- lxml (for faster HTML parsing)
- sqlite3 (built-in)

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 