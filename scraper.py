import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import Counter

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
subdomains = Counter()

def tokenize_text(text):
    """
    Simple regex-based word tokenizer from assignment 1
    """
    words = re.findall(r"[a-zA-Z0-9]+", text.lower())
    return [word for word in words 
            if word not in STOP_WORDS 
            and not word.isdigit() 
            and len(word) > 1]  # Filter out single characters

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    extracted_links = []
    
    # Check if the response is valid
    if resp.status != 200:
        return extracted_links

    try:
        # Get the defragmented URL
        defrag_url, _ = urldefrag(resp.url)
        
        # Add to unique pages if not seen before
        if defrag_url not in unique_pages:
            unique_pages.add(defrag_url)
            
            # Parse content with BeautifulSoup
            soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
                
            # Get text and count words
            text = soup.get_text()
            words = tokenize_text(text)
            
            # Update statistics
            page_word_counts[defrag_url] = len(words)
            word_frequencies.update(words)
            
            # Track subdomains for ics.uci.edu
            parsed_url = urlparse(defrag_url)
            if 'ics.uci.edu' in parsed_url.netloc:
                subdomains[parsed_url.netloc] += 1
        
        # Extract all links from the page
        for link in soup.find_all('a'):
            href = link.get('href')
            if href:
                # Convert relative URLs to absolute URLs
                absolute_url = urljoin(resp.url, href)
                # Remove fragments
                clean_url, _ = urldefrag(absolute_url)
                extracted_links.append(clean_url)
                
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        
    return extracted_links

def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
            
        # Check if URL is within allowed domains
        if not any(domain in parsed.netloc for domain in [
            'ics.uci.edu',
            'cs.uci.edu',
            'informatics.uci.edu',
            'stat.uci.edu'
        ]):
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
            return False
            
        return True

    except TypeError:
        print("TypeError for", parsed)
        return False

def get_analytics():
    """Return analytics about the crawl"""
    return {
        'unique_pages': len(unique_pages),
        'longest_page': max(page_word_counts.items(), key=lambda x: x[1]) if page_word_counts else None,
        'common_words': word_frequencies.most_common(50),
        'subdomains': sorted([(domain, count) for domain, count in subdomains.items()], key=lambda x: x[0])
    }
