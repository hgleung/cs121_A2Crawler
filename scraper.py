import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import Counter, defaultdict
import hashlib
import os
import json
from threading import Lock
import time

# Ensure report directory exists
REPORT_DIR = "report"
os.makedirs(REPORT_DIR, exist_ok=True)

# Common English stop words
STOP_WORDS = {
    'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and',
    'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being',
    'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't",
    'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during',
    'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't",
    'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here',
    "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i',
    "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's",
    'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself',
    'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought',
    'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she',
    "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such',
    'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves',
    'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're",
    "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up',
    'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were',
    "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which',
    'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would',
    "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours',
    'yourself', 'yourselves'
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
                if "ics.uci.edu" in domain:
                    f.write(f"{domain}, {len(pages)}\n")

def tokenize_text(text):
    """Simple regex-based word tokenizer from assignment 1"""
    words = re.findall(r"[a-zA-Z0-9]+", text.lower())
    return [word for word in words 
            if word not in STOP_WORDS 
            and not word.isdigit() 
            and len(word) > 1]

def get_url_pattern(url):
    """Extract pattern from URL for trap detection"""
    parsed = urlparse(url)
    # Remove numbers from path to detect patterns like /page/1, /page/2
    path_pattern = re.sub(r'\d+', 'N', parsed.path)
    return f"{parsed.netloc}{path_pattern}"

def get_content_hash(text):
    """Generate hash of page content for similarity detection.
    Using hashlib for now; implement from scratch later."""
    # Only use the first 1000 words to avoid memory issues
    words = ' '.join(tokenize_text(text)[:1000])
    return hashlib.md5(words.encode('utf-8')).hexdigest()

def is_trap(url, content):
    """Detect if URL or content indicates a trap"""
    # Check URL pattern repetition
    pattern = get_url_pattern(url)
    url_patterns[pattern] += 1
    if url_patterns[pattern] > MAX_PATTERN_REPEAT:
        print(f"Trap detected: URL pattern {pattern} repeated too many times")
        return True
        
    # Check content similarity
    if content:
        content_hash = get_content_hash(content)
        similar_pages = content_hashes[content_hash]
        if len(similar_pages) >= MAX_SIMILAR_CONTENT:
            print(f"Trap detected: Too many similar pages with hash {content_hash}")
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

def scraper(url, resp):
    print(f"\nProcessing URL: {url}")
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]
    print(f"Found {len(links)} links, {len(valid_links)} valid")
    return valid_links

def extract_next_links(url, resp):
    extracted_links = []
    
    # Handle various response issues
    if not resp.raw_response:
        print(f"Skipping {url} due to no raw response")
        return extracted_links

    # Check if this was a successful response or a valid redirect
    if resp.status not in [200, 301, 302]:
        print(f"Skipping {url} due to status {resp.status}")
        return extracted_links
        
    # For redirects, make sure we ended up at a valid page
    final_status = resp.raw_response.status_code if hasattr(resp.raw_response, 'status_code') else resp.status
    if final_status != 200:
        print(f"Skipping {url} due to final status {final_status}")
        return extracted_links
        
    if resp.raw_response.content is None:
        print(f"Skipping {url} due to empty response")
        return extracted_links

    try:
        # Handle redirects by using the final URL
        final_url = resp.raw_response.url if resp.raw_response.url else url
        defrag_url, _ = urldefrag(final_url)
        
        # Parse content
        try:
            soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        except Exception as e:
            print(f"Error parsing HTML for {url}: {str(e)}")
            return extracted_links
        
        # Remove script, style, and other non-content elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript', 'meta', 'link']):
            element.decompose()
            
        # Get text content
        text = soup.get_text(separator=' ', strip=True)
        words = tokenize_text(text)
        
        # Skip pages with too little content
        if len(words) < MIN_WORDS_PER_PAGE:
            print(f"Skipping {url} due to insufficient content: {len(words)} words")
            return extracted_links
            
        # Check for traps
        if is_trap(defrag_url, text):
            return extracted_links
            
        # Process valid page and update statistics
        update_stats(defrag_url, words)
        
        # Extract links
        seen_urls = set()  # Track URLs we've seen in this page
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            try:
                # Convert relative URLs to absolute
                absolute_url = urljoin(final_url, href)
                # Remove fragments
                clean_url, _ = urldefrag(absolute_url)
                
                # Skip if we've seen this URL in this page
                if clean_url in seen_urls:
                    continue
                    
                seen_urls.add(clean_url)
                extracted_links.append(clean_url)
                
            except Exception as e:
                print(f"Error processing link {href}: {str(e)}")
                continue
                
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        
    return extracted_links

def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            print(f"Rejecting {url}: invalid scheme {parsed.scheme}")
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
        
        # The domain must contain one of the valid domains
        if not any(domain in netloc for domain in valid_domains):
            print(f"Rejecting {url}: domain {netloc} not in allowed list")
            return False
            
        # Check for file extensions to avoid
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            print(f"Rejecting {url}: invalid file extension")
            return False
            
        return True

    except TypeError:
        print(f"TypeError for {url}")
        return False
    except Exception as e:
        print(f"Error validating {url}: {str(e)}")
        return False
