import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import Counter, defaultdict
import hashlib
import os
from threading import Lock
import logging

# Ensure report directory exists
REPORT_DIR = "report"
os.makedirs(REPORT_DIR, exist_ok=True)

# Set up logging to both file and console
log_file = os.path.join(REPORT_DIR, f"crawler_log.txt")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Replace print statements with logging
def log_info(message):
    logging.info(message)

# Common English stop words
STOP_WORDS = {
    'a', 'also', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and',
    'any', 'are', "aren", 'as', 'at', 'be', 'because', 'been', 'before', 'being',
    'below', 'between', 'both', 'but', 'by', "can", 'cannot', 'could', "couldn",
    'did', "didn", 'do', 'does', "doesn", 'doing', "don", 'down', 'during',
    'each', 'few', 'for', 'from', 'further', 'had', "hadn", 'has', "hasn",
    'have', 'having', 'he', 'her', 'here',
    'hers', 'herself', 'him', 'himself', 'his', 'how', 'i',
    'if', 'in', 'into', 'is', "isn", 'it',
    'its', 'itself', "let", 'me', 'may', 'more', 'most', "mustn", 'my', 'myself',
    'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought',
    'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan", 'she',
    'should', "shouldn", 'so', 'some', 'such',
    'than', 'that', 'the', 'their', 'theirs', 'them', 'themselves',
    'then', 'there', 'these', 'they',
    'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up',
    'very', 'was', "wasn", 'we', 'were',
    "weren", 'what', 'when', 'where', 'which',
    'while', 'who', 'whom', 'why', 'will', 'with', 'would',
    "wouldn", 'you', 'your', 'yours',
    'yourself', 'yourselves'
}

# Non-words that should be ignored, such as http, www or extensions like xml, dav (ICS is allowed)
NON_WORDS = {
    'http', 'https', 'xml', 'www', 'edu', 'dav', 'bdd', 'll', 're', 've', 'cfg', 'simse', 'sc', 'dt', 'markellekelly', 'br'
}

# Global variables to track statistics
unique_pages = set()
page_word_counts = {}
word_frequencies = Counter()
subdomains = defaultdict(set)  # Changed to track unique pages per subdomain
report_lock = Lock()  # Thread safety for report writing
url_patterns = defaultdict(int)  # Track URL pattern frequencies
content_hashes = defaultdict(list)  # Track content similarity
MAX_PATTERN_REPEAT = 10  # Maximum times a URL pattern can repeat
MAX_SIMILAR_CONTENT = 5  # Maximum number of pages with similar content
MIN_WORDS_PER_PAGE = 50  # Minimum words for a page to be considered content-rich

def update_reports():
    """Update all report files with current statistics"""
    with report_lock:
        # 1. Unique pages report
        with open(os.path.join(REPORT_DIR, "unique_pages.txt"), "w") as f:
            f.write(f"Number of unique pages found: {len(unique_pages)}\n\n")
            f.write("List of unique pages:\n")
            for page in sorted(unique_pages):
                f.write(f"{page}\n")

        # 2. Longest page report
        if page_word_counts:
            longest_url = max(page_word_counts.items(), key=lambda x: x[1])
            with open(os.path.join(REPORT_DIR, "longest_page.txt"), "w") as f:
                f.write(f"URL: {longest_url[0]}\n")
                f.write(f"Word count: {longest_url[1]}\n")

        # 3. Common words report
        with open(os.path.join(REPORT_DIR, "common_words.txt"), "w") as f:
            f.write("50 most common words and their frequencies:\n")
            for word, freq in word_frequencies.most_common(50):
                f.write(f"{word}: {freq}\n")

        # 4. Subdomains report
        with open(os.path.join(REPORT_DIR, "subdomains.txt"), "w") as f:
            f.write("Subdomains of ics.uci.edu and their unique page counts:\n")
            # Sort subdomains alphabetically
            sorted_subdomains = sorted(subdomains.items(), key=lambda x: x[0])
            for domain, pages in sorted_subdomains:
                # Match ics.uci.edu or *.ics.uci.edu but not informatics.uci.edu
                if re.match(r'^(?!informatics\.)([\w-]+\.)?ics\.uci\.edu$', domain):
                    f.write(f"{domain}, {len(pages)}\n")

def tokenize_text(text):
    """Simple regex-based word tokenizer from assignment 1"""
    words = re.findall(r"[a-zA-Z]+", text.lower())
    return [word for word in words 
            if word not in STOP_WORDS 
            and word not in NON_WORDS
            and len(word) > 1]

def get_url_pattern(url):
    """Extract pattern from URL for trap detection"""
    parsed = urlparse(url)
    
    # Special handling for wiki URLs
    if 'wiki.ics.uci.edu' in parsed.netloc or 'swiki.ics.uci.edu' in parsed.netloc:
        # Extract the base wiki page path without query parameters
        path_parts = parsed.path.split('/')
        if 'doku.php' in path_parts:
            # Get the actual wiki page path after doku.php
            doku_index = path_parts.index('doku.php')
            wiki_path = '/'.join(path_parts[doku_index + 1:]) if doku_index + 1 < len(path_parts) else ''
            
            # If there's a query string, only keep the page ID if present
            if parsed.query:
                query_params = dict(param.split('=', 1) for param in parsed.query.split('&') if '=' in param)
                page_id = query_params.get('id', '')
                if page_id:
                    wiki_path = page_id
            
            return f"{parsed.netloc}/wiki/{wiki_path}"
        return f"{parsed.netloc}{parsed.path}"
    
    # Special handling for paths containing year ranges (e.g., department-seminars-2013-2014)
    if 'department-seminars-' in parsed.path:
        # Keep the year ranges in seminar archive paths
        path_parts = parsed.path.split('/')
        path_pattern = []
        for part in path_parts:
            if part.startswith('department-seminars-') and re.search(r'\d{4}-\d{4}', part):
                path_pattern.append(part)  # Keep the entire year range intact
            else:
                path_pattern.append(re.sub(r'\d+', 'N', part))
        path_pattern = '/'.join(path_pattern)
    else:
        # Normal path handling for other URLs
        path_pattern = re.sub(r'\d+', 'N', parsed.path)
    
    # Handle query parameters
    if parsed.query:
        # Parse query parameters
        query_params = {}
        for param in parsed.query.split('&'):
            if '=' in param:
                key, value = param.split('=', 1)
                # Keep specific query parameters intact
                if key in ['seminar_id', 'event_id', 'post_id', 'page_id', 'archive_year']:
                    query_params[key] = value
                else:
                    # Replace numbers with N in other parameter values
                    query_params[key] = re.sub(r'\d+', 'N', value)
        
        # Reconstruct query string with sorted parameters for consistent comparison
        query_pattern = '&'.join(f"{k}={v}" for k, v in sorted(query_params.items()))
        return f"{parsed.netloc}{path_pattern}?{query_pattern}"
    
    return f"{parsed.netloc}{path_pattern}"

def get_content_hash(text):
    """Generate hash of page content for similarity detection."""
    words = tokenize_text(text)
    
    # Skip very short content
    if len(words) < 20:
        return None
        
    # For archive.ics.uci.edu URLs with search parameters, be more strict
    if 'archive.ics.uci.edu' in text and ('search=' in text or 'Keywords=' in text):
        # For search pages, create a more detailed hash
        all_words = ' '.join(words)
        return hashlib.md5(all_words.encode('utf-8')).hexdigest()
    
    # For other pages, use a sample of words from different parts of the content
    word_count = len(words)
    if word_count <= 1000:
        sampled_words = words
    else:
        # Take words from the beginning, middle, and end to catch differences
        start = words[:300]
        middle = words[word_count//2-150:word_count//2+150]
        end = words[-300:]
        sampled_words = start + middle + end
    
    return hashlib.md5(' '.join(sampled_words).encode('utf-8')).hexdigest()

def is_trap(url, content):
    """Detect if URL or content indicates a trap"""
    # Check URL pattern repetition
    pattern = get_url_pattern(url)
    url_patterns[pattern] += 1
    if url_patterns[pattern] > MAX_PATTERN_REPEAT:
        log_info(f"Trap detected: URL pattern {pattern} repeated too many times")
        return True
        
    # Check content similarity
    if content:
        content_hash = get_content_hash(content)
        if content_hash is None:
            return False  # Skip similarity check for very short content
            
        similar_pages = content_hashes[content_hash]
        if len(similar_pages) >= MAX_SIMILAR_CONTENT:
            log_info(f"Trap detected: Too many similar pages with hash {content_hash}")
            log_info(f"Similar pages: {', '.join(similar_pages)}")
            return True
        similar_pages.append(url)
    
    return False

def update_stats(url, words):
    """Update statistics and reports"""
    # Update unique pages
    unique_pages.add(url)
    
    # Update word count for the page
    page_word_counts[url] = len(words)
    
    # Update word frequencies
    word_frequencies.update(words)
    
    # Update subdomain statistics
    parsed_url = urlparse(url)
    netloc = parsed_url.netloc.lower()
    subdomains[netloc].add(url)
    
    # Update report files
    update_reports()

def log_cache_error(url, status, response):
    """Log 6XX status codes which are specific cache server responses"""
    try:
        with report_lock:
            # Ensure the report directory exists
            os.makedirs(REPORT_DIR, exist_ok=True)
            
            cache_error_file = os.path.join(REPORT_DIR, "cache_errors.txt")
            # Create the file if it doesn't exist
            if not os.path.exists(cache_error_file):
                open(cache_error_file, 'w').close()
                
            with open(cache_error_file, "a") as f:
                f.write(f"\nURL: {url}\n")
                f.write(f"Status Code: {status}\n")
                # Log the raw response content if available
                if hasattr(response, 'raw_response') and hasattr(response.raw_response, 'content'):
                    try:
                        content = response.raw_response.content.decode('utf-8')
                        f.write(f"Response Content: {content}\n")
                    except:
                        f.write("Response Content: [Unable to decode response content]\n")
                f.write("-" * 80 + "\n")
                f.flush()  # Ensure the content is written immediately
    except Exception as e:
        log_info(f"Error logging cache error for {url}: {str(e)}")

def scraper(url, resp):
    log_info(f"\nProcessing URL: {url}")
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]
    log_info(f"Found {len(links)} links, {len(valid_links)} valid")
    return valid_links

def extract_next_links(url, resp):
    extracted_links = []
    
    # Handle various response issues
    if not resp.raw_response:
        log_info(f"Skipping {url} due to no raw response")
        return extracted_links

    # Check content type for non-HTML content
    content_type = resp.raw_response.headers.get('Content-Type', '').lower()
    if 'text/html' not in content_type:
        if any(t in content_type for t in ['application/pdf', 'application/x-pdf', 'application/acrobat']):
            log_info(f"Skipping {url}: PDF content detected via Content-Type: {content_type}")
        else:
            log_info(f"Skipping {url}: non-HTML content type: {content_type}")
        return extracted_links

    # Check if this was a successful response
    if resp.status != 200:
        # Log 6XX status codes specifically
        if 600 <= resp.status < 700:
            log_info(f"Cache server error for {url} with status {resp.status}")
            log_cache_error(url, resp.status, resp)
        log_info(f"Skipping {url} due to status {resp.status}")
        return extracted_links

    try:
        # Get the final URL after any redirects for resolving relative links
        final_url = resp.raw_response.url
        # Use original URL for statistics to preserve the URL that was actually crawled
        defrag_url, _ = urldefrag(url)  # Use original URL instead of final_url
        
        # Parse content
        try:
            soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        except Exception as e:
            log_info(f"Error parsing HTML for {url}: {str(e)}")
            return extracted_links
        
        # Remove script, style, and other non-content elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript', 'meta', 'link']):
            element.decompose()
            
        # Get text content
        text = soup.get_text(separator=' ', strip=True)
        words = tokenize_text(text)
        
        # Skip pages with too little content
        if len(words) < MIN_WORDS_PER_PAGE:
            log_info(f"Skipping {url} due to insufficient content: {len(words)} words")
            return extracted_links
            
        # Check for traps
        if is_trap(defrag_url, text):
            return extracted_links
            
        # Process valid page and update statistics
        update_stats(defrag_url, words)
        
        # Extract links
        seen_urls = set()  # Track URLs we've seen in this page
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            if not href:
                continue
                
            try:
                # Resolve relative URLs against the final URL after redirects
                absolute_url = urljoin(final_url, href)
                # Remove fragments from the resolved URL
                clean_url, _ = urldefrag(absolute_url)
                if clean_url not in extracted_links:
                    extracted_links.append(clean_url)
            except Exception as e:
                log_info(f"Error processing link {href}: {str(e)}")
                continue
                
    except Exception as e:
        log_info(f"Error processing {url}: {str(e)}")
        
    return extracted_links

def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            log_info(f"Rejecting {url}: invalid scheme {parsed.scheme}")
            return False

        # Check if URL is within allowed domains
        netloc = parsed.netloc.lower()
        
        # Check for the allowed domain patterns
        valid_domains = [
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu"
        ]
        
        # The domain must exactly match one of the valid domains at the end of netloc
        # This prevents matching substrings in paths or subdomains of other sites
        if not any((netloc == domain or netloc.endswith("." + domain)) 
                  for domain in valid_domains):
            log_info(f"Rejecting {url}: domain {netloc} not in allowed list")
            return False
            
        # Special handling for cbcl.ics.uci.edu URLs
        if 'cbcl.ics.uci.edu' in netloc:
            # Block diff views, edit pages, backlinks and other problematic actions
            if parsed.query:
                query_params = dict(param.split('=', 1) for param in parsed.query.split('&') if '=' in param)
                if any(param in query_params for param in ['do']):
                    action = query_params.get('do', '')
                    if action in ['login', 'recent', 'revisions', 'diff', 'edit', 'backlink', 'resendpwd', 'index', '']:
                        log_info(f"Rejecting {url}: cbcl action parameter detected: {action}")
                        return False
                
                # Check for PDFs in the id parameter
                if 'id' in query_params and '.pdf' in query_params['id'].lower():
                    log_info(f"Rejecting {url}: PDF document referenced in id parameter")
                    return False

            # Block URLs that seem to encode external links in the path
            if '/http/' in parsed.path.lower() or '/www.' in parsed.path.lower():
                log_info(f"Rejecting {url}: external link encoded in path")
                return False

        # Special handling for ics.uci.edu/people/ URLs with filters
        if 'ics.uci.edu' in netloc and '/people/' in parsed.path.lower():
            if parsed.query and 'filter' in parsed.query.lower():
                log_info(f"Rejecting {url}: ics.uci.edu people filter detected")
                return False

        # Special handling for grape.ics.uci.edu wiki URLs
        if 'grape.ics.uci.edu' in netloc and '/wiki/' in parsed.path:
            if parsed.query:
                query_params = dict(param.split('=', 1) for param in parsed.query.split('&') if '=' in param)
                # Block version parameters and diff actions
                if 'version' in query_params or 'action' in query_params:
                    log_info(f"Rejecting {url}: grape wiki version/action parameter detected")
                    return False

        # Filter directory sorting parameters
        if parsed.query:
            # Check for directory sorting parameters (C=N|M|S|D for Name|Modified|Size|Description, O=A|D for Ascending|Descending)
            if any(param.startswith('C=') or param.startswith('O=') for param in parsed.query.split(';')):
                log_info(f"Rejecting {url}: directory sorting parameters detected")
                return False

        # Special handling for wiki URLs
        if ('wiki.ics.uci.edu' in netloc or 'swiki.ics.uci.edu' in netloc):
            # Block problematic wiki query parameters that create duplicate content
            if parsed.query:
                query_params = dict(param.split('=', 1) for param in parsed.query.split('&') if '=' in param)
                # Block certain actions and views that duplicate content
                if any(param in query_params for param in ['do', 'rev', 'tab_files', 'tab_details', 'image']):
                    log_info(f"Rejecting {url}: wiki action/view parameter detected")
                    return False
                # Block media namespace and other utility pages
                if query_params.get('ns') in ['projects', 'media', 'wiki', 'windows']:
                    log_info(f"Rejecting {url}: wiki utility namespace detected")
                    return False
            
        # Check for potential PDF files that don't end in .pdf
        path_lower = parsed.path.lower()
        if any(pdf_indicator in path_lower for pdf_indicator in ['/pdf/', '/pdfs/', '/files/pdf/']):
            log_info(f"Rejecting {url}: likely PDF document based on path")
            return False
            
        # Check for file extensions to avoid
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|pps|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1|sql|mpg"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", path_lower):
            log_info(f"Rejecting {url}: invalid file extension")
            return False
            
        # Check for problematic query strings that might cause infinite loops
        if parsed.query:
            # Check for filter parameters that might create duplicate content
            if any(param.startswith('filter') for param in parsed.query.split('&')):
                # Count the number of filter parameters
                filter_count = sum(1 for param in parsed.query.split('&') if param.startswith('filter'))
                if filter_count >= 2:  # If there are multiple filter parameters, likely a trap
                    log_info(f"Rejecting {url}: contains multiple filter parameters in query string")
                    return False
                    
        return True

    except TypeError:
        log_info(f"TypeError for {url}")
        return False
    except Exception as e:
        log_info(f"Error validating {url}: {str(e)}")
        return False
