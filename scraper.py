import re
from collections import Counter
from urllib.parse import urljoin, urlparse
from nltk.corpus import stopwords
from datasketch import MinHash
from bs4 import BeautifulSoup

MAX_CONTENT_LENGTH = 10000000 # 10MB

def scraper(url, resp):
    """
    Scrapes the given URL and returns a list of links and a dictionary of words

    Parameters:
        url (str): url to scrape
        resp (Response): response from downloading url
    
    Returns:
        tuple: tuple containing:
            list: list of links
            dict: Counter of words
    """
    words = None
    links = []
    m = None

    # Check if content length is too large
    content_length = resp.raw_response.headers.get('Content-Length')
    if content_length and int(content_length) > MAX_CONTENT_LENGTH:
        return (links, words, m)

    content_type = resp.raw_response.headers.get('Content-Type', '').lower()
    if "text/html" in content_type:
        # Extract text content from HTML
        # Extract links and words from the response
        links, text = extract_text_and_next_links(url, resp)

        # Count words in the text
        words, m = count_words(text)

    return ([link for link in links if is_valid(link)], words, m)

def count_words(text):
    """
    Counts the number of times each word appears in the given text
    and computes the minhash for the text

    Parameters:
        text (str): text to count words from
    
    Returns:
        tuple: tuple containing:
            dict: Counter of words
            MinHash: MinHash of words
    """
    stop_words = set(stopwords.words('english'))
    words = re.findall(r'\b\w+\b', text.lower())
    words = [word for word in words if word not in stop_words]

    # Compute word count and minhash for the text
    word_counter = Counter(words)
    m = MinHash(num_perm=90)
    for word, count in word_counter.items():
        m.update(word.encode('utf8'))
    return word_counter, m


def extract_text_and_next_links(url, resp):
    """
    Extract links from the given URL and the text.

    Parameters:
        url (str): url to extract links from
        resp (Response): response from downloading url
    
    Returns:
        list: list of links
    """
    if not resp.raw_response:
        return []

    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    links = []

    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if href:
            full_url = urljoin(url, href)
            links.append(full_url)

    return links, soup.get_text()

def is_valid(url):
    """
    Checks if the URL is valid based on the given requirements.

    Args:
        url (str): The URL to be checked.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    allowed_domains = [
        r"^.+\.ics\.uci\.edu(/.*)?$",
        r"^.+\.cs\.uci\.edu(/.*)?$",
        r"^.+\.informatics\.uci\.edu(/.*)?$",
        r"^.+\.stat\.uci\.edu(/.*)?$"
    ]

    try:

        parsed = urlparse(url)._replace(fragment='')
        if not parsed.scheme or not parsed.hostname:
            return False

        # Check if the URL belongs to one of the allowed domains
        valid_domain = any(re.match(domain, url) for domain in allowed_domains)
        if not valid_domain:
            return False

        # Additional checks for URL patterns
        # Add more checks here if needed
        return not re.match(r".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4"
                            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                            r"|epub|dll|cnf|tgz|sha1"
                            r"|thmx|mso|arff|rtf|jar|csv"
                            r"|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", url.lower())

    except TypeError:
        return False


if __name__ == "__main__":
    print(is_valid("http://www.ics.uci.edu"))
    print(is_valid("http://www.cs.uci.edu"))
    print(is_valid("http://www.informatics.uci.edu"))
    print(is_valid("http://www.stat.uci.edu/?page_id=352#fragment"))
    print(is_valid("http://www.test.ics.uci.edu/ex.html#fake"))
    print(is_valid("http://www.anything.cs.uci.edu/"))
    print(is_valid("http://www.what.informatics.uci.edu"))
    print(is_valid("http://www.help.stat.uci.edu/"))
