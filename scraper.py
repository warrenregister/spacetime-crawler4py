import re
from collections import Counter
from urllib.parse import urljoin, urlparse, parse_qs
#from nltk.corpus import stopwords
from bs4 import BeautifulSoup
from crawler.simhash import SimHash
from nltk import download

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
    links = []

    # Check if content length is too large
    content_length = resp.raw_response.headers.get('Content-Length')
    if content_length and int(content_length) > MAX_CONTENT_LENGTH:
        return links

    content_type = resp.raw_response.headers.get('Content-Type', '').lower()
    if "text/html" in content_type:
        # Extract text content from HTML
        # Extract links and words from the response
        links, text = extract_text_and_next_links(url, resp)


    return [link for link in links if is_valid(link)]


# TODO: Modify to only extract relevant text content from wikie pages
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

    # Remove header, footer, and nav tags to reduce repeat link adds
    for tag in soup.find_all(['header', 'footer', 'nav']):
        tag.decompose()
    links = []

    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if href:
            full_url = urljoin(url, href)
            links.append(full_url)
    
    # Remove script and style contents
    for tag in soup(['script', 'style']):
        tag.decompose()

    return links, soup.get_text()


# FIXME: Modify this to only validate urls that look like poewiki.net/wiki/{anything}
def is_valid(url):
    """
    Checks if the URL is valid based on the given requirements.

    Args:
        url (str): The URL to be checked.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    # allowed_domains = [
    #     r"^https?://([a-zA-Z0-9.-]*\.)?ics\.uci\.edu(:[0-9]+)?(/[a-zA-Z0-9_\-./;?%&=+#]*)?$",
    #     r"^https?://([a-zA-Z0-9.-]*\.)?cs\.uci\.edu(:[0-9]+)?(/[a-zA-Z0-9_\-./;?%&=+#]*)?$",
    #     r"^https?://([a-zA-Z0-9.-]*\.)?informatics\.uci\.edu(:[0-9]+)?(/[a-zA-Z0-9_\-./;?%&=+#]*)?$",
    #     r"^https?://([a-zA-Z0-9.-]*\.)?stat\.uci\.edu(:[0-9]+)?(/[a-zA-Z0-9_\-./;?%&=+#]*)?$"
    # ]
    
    # Only allow domains from poewiki.net to any page that looks like poewiki.net/wiki/{anything}
    allowed_domains = [r"https?://poewiki\.net/wiki/[a-zA-Z0-9_\-./;?%&=+#]*?"]


    try:

        parsed = urlparse(url)._replace(fragment='')
        if not parsed.scheme or not parsed.hostname:
            return False

        # Check if the URL belongs to one of the allowed domains
        valid_domain = any(re.match(domain, url) for domain in allowed_domains)
        if not valid_domain:
            return False
        
        is_trap , trap_type = is_infinite_trap(url)
        if is_trap is True:
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

# FIXME: Remove references to is_infinite_trap or make it handle infinite trap situations which come up
# def is_infinite_trap(url):
#     trap_patterns = {
#         "path": [
#             # Script related
#             r'\b(cgi-bin|\.aspx|\.jsp|\.cgi|\.ipynb|\.py|\.cpp|\.hpp|\.jar|\.js|\.sql|\.png|\.jpg|\.jpeg|\.pdf|\.txt|\.docx|\.doc|\.xls|\.xlsx|\.ppt|\.pptx|\.gif|\.mp3|\.mp4|\.avi|\.mov|\.mpeg|\.ram|\.m4v|\.mkv|\.ogg|\.ogv|\.ps|\.eps|\.tex|\.exe|\.bz2|\.tar|\.msi|\.bin|\.7z|\.psd|\.dmg|\.iso|\.epub|\.dll|\.cnf|\.tgz|\.sha1|\.thmx|\.mso|\.arff|\.rtf|\.jar|\.csv|\.rm|\.smil|\.wmv|\.swf|\.wma|\.zip|\.rar|\.gz|\.pdf)\b',
#             r'(/\d{5,}/)|(^/\d{5,}$)',
#             r'\/(?:event|calendar|datasets|events|dataset|zip-attachment|attachment|raw-attachment)(\/|$)',
#         ],
#         "query": [
#                     # Calendars
#                     r'(19[0-9]{2}|2[0-9]{3})/(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])',

#                     # Ordering and filtering related
#                     r'(filter|limit|order|sort|version|precision|ucnetid|from|to|department|letter|form|forms|id|action|format)=',

#                     # Table views
#                     r'view=table',

#                     # Session related
#                     r'(sessionid|session_id|SID|PHPSESSID|JSESSIONID|ASPSESSIONID|sid|view)=',

#                     # Social media sites
#                     r'(?:\?|&|=)(?:twitter|facebook|tiktok|instagram)',
#                     ]

#     }

#     parsed_url = urlparse(url)
#     parameters = parse_qs(parsed_url.query)
#     if len(parameters) > 3:
#         return True, "Too many parameters"
    
#     for section, patterns in trap_patterns.items():
#         target = getattr(parsed_url, section)
#         # if section == "query":  # if we are checking params, parse them first
#         #     target = str(parse_qs(target))  # parse the parameters and convert to string
#         for i, pattern in enumerate(patterns):
#             if re.search(pattern, target) or len(target) > 400:
#                 if section == 'query' and i in [ 2, 4]:
#                     pass
#                 return True, trap_patterns[section][i]
#     return False, None

if __name__ == "__main__":
    test = 'https://wics.ics.uci.edu/wics-crepe-boothing?whatever=twitter.com'
    print(is_infinite_trap(test))
