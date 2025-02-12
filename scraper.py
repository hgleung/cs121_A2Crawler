import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
from collections import Counter

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

# Global variables to track statistics
unique_pages = set()
page_word_counts = {}
word_frequencies = Counter()
subdomains = Counter()

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
            words = nltk.word_tokenize(text.lower())
            stop_words = set(stopwords.words('english'))
            filtered_words = [word for word in words if word.isalnum() and word not in stop_words]
            
            # Update statistics
            page_word_counts[defrag_url] = len(filtered_words)
            word_frequencies.update(filtered_words)
            
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
