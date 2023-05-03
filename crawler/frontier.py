import os
import shelve
import time
from collections import Counter, defaultdict
from queue import Empty, Queue
from threading import RLock
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from io import StringIO
from lxml import etree

from scraper import is_valid
from utils.download import download
from utils import get_logger, get_urlhash, normalize
from datasketch import MinHashLSH

# Remember to not visit the cache website yet
class Frontier(object):
    """
    Frontier class for frontier set and frontier queue.

    Attributes:
        politeness_delay (float): delay between requests to same domain
        domains (dict): dict of Queue for each domaincond
        last_request_time (dict): time of last request to each domain
        lock (RLock): lock for thread safety
        logger (Logger): logger instance
        config (Config): configuration instance
        save (shelve): save file instance
        word_count (Counter): word count for frontier set
        subdomains (set): set of subdomains
    """
    def __init__(self, config, restart):
        """
        Initialize Frontier.

        Parameters:
            config (Config): configuration instance
            restart (bool): whether to restart from seed
        """
        self.politeness_delay = config.politeness_delay
        self.domains_to_scrape = defaultdict(Queue) # frontier queue for each domain
        self.last_request_time = {} # time of last request to each domain
        self.lock = RLock() # general lock for all other shared resources
        self.robots_parsers_lock = RLock() # lock for robots_parsers
        self.lsh_lock = RLock() # lock for lsh
        self.sitemaps_lock = RLock() # lock for sitemaps
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.word_count = Counter() # word count for frontier set
        self.subdomains = set() # set of subdomains
        self.robots_parsers = {} # dict of robots parsers for each domain
        self.sitemaps = defaultdict(set) # dict of sitemaps for each domain
        self.lsh = MinHashLSH(threshold=0.6, num_perm=90)


        if not os.path.exists(self.config.save_file + '.db') and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file + '.db') and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove('' + self.config.save_file + '.db')

        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        """
        Parse save file and add urls to frontier.
        """
        with self.lock:
            total_count = len(self.save)
            tbd_count = 0
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    self.add_url(url)
                    tbd_count += 1
            self.logger.info(
                f"Found {tbd_count} urls to be downloaded from {total_count} "
                f"total urls discovered.")
    
    def get_tbd_url(self):
        """
        Get next url to be downloaded.

        Returns:
            str: next url to be downloaded
        """
        with self.lock:
            while len(self.domains_to_scrape) > 0:
                for domain, url_queue in list(self.domains_to_scrape.items()):
                    last_request_time = self.last_request_time.get(domain, 0)
                    time_since_last_request = time.time() - last_request_time

                    if time_since_last_request >= self.politeness_delay:
                        try:
                            url = url_queue.get(block=False)
                            self.last_request_time[domain] = time.time()
                            return url
                        except Empty:
                            del self.domains_to_scrape[domain]
                time.sleep(self.politeness_delay)
    
    def add_url(self, url, scraped=False):
        """
        Add url to frontier.

        Parameters:
            url (str): url to add to frontier
            scraped (bool): whether url has been scraped
        """
        url = normalize(url)
        domain = urlparse(url).netloc

        with self.lock and self.robots_parsers_lock:
            # Check if the domain is new -> fetch robots.txt and process sitemaps
            if domain not in self.subdomains:
                parser = self.get_robots_txt_parser(url)
                self.robots_parsers[domain] = parser
                sitemap_urls = self.get_sitemap_urls_from_robots_txt(url)
                self.process_sitemaps(sitemap_urls)

                # Add the new domain to subdomains set
                self.subdomains.add(domain)
            else:
                parser = self.robots_parsers[domain]

            if not parser.can_fetch(self.config.user_agent, url): 
                return

            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.save[urlhash] = (url, scraped)
                self.save.sync()
                self.domains_to_scrape[domain].put(url)

    def is_similar(self, minhash):
        """
        Check if url is too similar to others in frontier.

        Parameters:
            minhash (int): simhash value for the url's content
        
        Returns:
            bool: whether url is too similar to others in frontier
        """
        with self.lsh_lock:
            similarity = self.lsh.is_similar(minhash)
            if similarity is True:
                return True
            self.lsh.insert(minhash)
        return False

    def mark_url_complete(self, url):
        """
        Mark url as completed.

        Parameters:
            url (str): url to mark as completed
        """
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()
    
    def add_words(self, words: Counter):
        """
        Add words to word count.

        Parameters:
            words (Counter): words to add to word count
        """
        with self.lock:
            self.word_count += words
    
    def get_robots_txt_parser(self, url):
        """
        Get robots.txt parser for domain of url.

        Parameters:
            url (str): url to get robots.txt parser for
        
        Returns:
            RobotFileParser: robots.txt parser for domain of url
        """
        url = normalize(url)
        domain = urlparse(url).netloc
        with self.robots_parsers_lock:
            if domain not in self.robots_parsers:
                self.robots_parsers[domain] = self.download_robots_txt_parser_for_domain(domain)
            return self.robots_parsers[domain]
    
    def download_robots_txt_parser_for_domain(self, domain):
        """
        Get robots.txt parser for domain.

        Parameters:
            domain (str): domain to get robots.txt parser for
            config (Config): A Config object containing the crawler configuration.

        Returns:
            RobotFileParser: robots.txt parser for domain
        """
        robots_url = urljoin(f"https://{domain}", "robots.txt")
        self.add_url(robots_url, scraped=True)
        resp = download(robots_url, self.config)


        parser = RobotFileParser()
        if resp.status == 200 and resp.raw_response is not None:
            robots_txt_content = resp.raw_response.decode()
            parser.parse(StringIO(robots_txt_content).readlines())
        else:
            self.logger.error(f"Error getting robots.txt from {domain}")

        return parser

    def get_sitemap_urls_from_robots_txt(self, url):
        """
        Get sitemap urls from robots.txt for domain of url.

        Parameters:
            url (str): url to get sitemap urls for
        
        Returns:
            list: list of sitemap urls
        """
        domain = urlparse(url).netloc
        with self.robots_parsers_lock and self.sitemaps_lock:
            if domain in self.robots_parsers.keys():
                parser = self.robots_parsers[domain]
            else:
                parser = self.get_robots_txt_parser(url)
            
            sitemap_urls = []
            for link in parser.sitemaps:
                self.sitemaps[domain].add(link)
                sitemap_urls.append(link)

        return sitemap_urls
    
    def process_sitemaps(self, sitemap_urls):
        """
        Process a list of sitemap urls, add new found urls to frontier.

        Parameters:
            sitemap_urls (list): list of sitemap urls
        """
        for sitemap_url in sitemap_urls:
            domain = urlparse(sitemap_url).netloc
            with self.sitemaps_lock:
                if sitemap_url not in self.sitemaps[domain]:
                    self.sitemaps[domain].add(sitemap_url)
                    urls_from_sitemap = self.get_urls_from_sitemap(sitemap_url)
                    for url in urls_from_sitemap:
                        self.add_url(url)

    def get_urls_from_sitemap(self, sitemap_url):
        """
        Get urls from sitemap.

        Parameters:
            sitemap_url (str): url of sitemap
        Returns:
            list: list of urls from sitemap
        """
        resp = download(sitemap_url, self.config)

        if resp.status != 200:
            self.logger.error(f"Error downloading sitemap {sitemap_url}")
            return []

        try:
            root = etree.fromstring(resp.raw_response)
        except etree.XMLSyntaxError:
            self.logger.error(f"Error parsing sitemap {sitemap_url}")
            return []

        urls = []

        # Check if it's a sitemap index
        is_sitemap_index = False
        for sitemap_index in root.findall("sitemap", root.nsmap):
            is_sitemap_index = True
            loc_element = sitemap_index.find("loc", root.nsmap)
            if loc_element is not None:
                sitemap_urls = self.get_urls_from_sitemap(loc_element.text)
                urls.extend(sitemap_urls)

        # If it's not a sitemap index, it's a regular sitemap
        if not is_sitemap_index:
            for url_element in root.findall("url", root.nsmap):
                loc_element = url_element.find("loc", root.nsmap)
                if loc_element is not None:
                    urls.append(loc_element.text)

        return urls
