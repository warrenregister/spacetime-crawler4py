import re
from collections import Counter
from urllib.parse import urlsplit, urljoin, urlparse
from nltk.corpus import stopwords
from bs4 import BeautifulSoup


def get_domain(url):
    parsed_uri = urlparse(url)
    return f"{parsed_uri.scheme}://{parsed_uri.netloc}"


def fetch_robots_txt(url):
    domain = get_domain(url)
    robots_txt_url = urljoin(domain, "/robots.txt")
    response = requests.get(robots_txt_url)
    return response.text if response.status_code == 200 else ""


def parse_robots_txt(robots_txt, user_agent="*"):
    rules = {"User-agent": user_agent, "Disallow": [], "Crawl-delay": None}
    lines = robots_txt.strip().split("\n")

    for line in lines:
        line = line.strip()

        if line.startswith("User-agent"):
            agent = line.split(":")[1].strip()
            if agent == user_agent:
                rules["User-agent"] = agent
        elif line.startswith("Disallow"):
            path = line.split(":")[1].strip()
            rules["Disallow"].append(path)
        elif line.startswith("Crawl-delay"):
            delay = float(line.split(":")[1].strip())
            rules["Crawl-delay"] = delay

    return rules


def scraper(url, resp, robots_rules):
    links = extract_next_links(url, resp)
    words = None
    subdomain = None
    if resp.status != 200:
        return [], words, subdomain
    
    domain = get_domain(url)
    if domain not in robots_rules:
        robots_txt = fetch_robots_txt(url)
        rules = parse_robots_txt(robots_txt)
        robots_rules[domain] = rules
    
    content_type = resp.raw_response.headers.get('Content-Type', '').lower()
    if "text/html" in content_type:
        # Extract text content from HTML
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        text = soup.get_text()

        # Count words in the text
        words = count_words(text)

        # Store subdomains for ics.uci.edu
        if ".ics.uci.edu" in url:
            parsed_url = urlsplit(url)
            subdomain = f"{parsed_url.scheme}://{parsed_url.netloc}"

    return ([link for link in links if is_valid(link)], words, subdomain)

def count_words(text):
    stop_words = set(stopwords.words('english'))
    words = re.findall(r'\b\w+\b', text.lower())
    words = [word for word in words if word not in stop_words]
    return Counter(words)


def extract_next_links(url, resp):
    if resp.status != 200 or not resp.raw_response:
        return []

    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    links = []

    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if href:
            full_url = urljoin(url, href)
            links.append(full_url)

    return links

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
        parsed = urlparse(url)
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
    print(is_valid("http://www.stat.uci.edu"))
