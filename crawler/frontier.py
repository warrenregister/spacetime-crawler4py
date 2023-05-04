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
from simhash import SimhashIndex
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
        self.politeness_delay = config.time_delay
        self.domains_to_scrape = defaultdict(Queue) # frontier queue for each domain
        self.last_request_time = {} # time of last request to each domain
        self.lock = RLock() # general lock for all other shared resources
        self.robots_parsers_lock = RLock() # lock for robots_parsers
        self.shi_lock = RLock() # lock for simhash index
        self.sitemaps_lock = RLock() # lock for sitemaps
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.word_count = Counter() # word count for frontier set
        self.subdomains = {} # dict of subdomains and unique urls
        self.robots_parsers = {} # dict of robots parsers for each domain
        self.sitemaps = defaultdict(set) # dict of sitemaps for each domain
        self.simhash_index = SimhashIndex([], k=3)
        self.max_words = (None, 0)
        self.save_interval = 100  # Replace n with the desired interval
        self.links_processed = 0

        self.handle_shelves(restart)

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
            self.links_processed += 1
            self.maybe_save_data()
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
            
            self.save_data()
            return None
    
    def add_url(self, url, scraped=False):
        """
        Add url to frontier.

        Parameters:
            url (str): url to add to frontier
            scraped (bool): whether url has been scraped
        """
        url = normalize(url)

        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        fragmentless_url = parsed_url._replace(fragment="").geturl()
        urlhash = get_urlhash(url)


        with self.lock and self.robots_parsers_lock:
            # Check if the url is already in the frontier
            if urlhash in self.save:
                self.logger.info(f"URL {url} already in frontier.")
                return
            # Check if the domain is new -> fetch robots.txt and process sitemaps
            if domain not in self.robots_parsers.keys():
                parser = self.get_robots_txt_parser(url)
                self.robots_parsers[domain] = parser
                self.get_sitemap_urls_from_robots_txt(url)  
            else:
                parser = self.robots_parsers[domain]
            
            # add url to subdomains
            if parsed_url.hostname not in self.subdomains.keys():
                self.subdomains[parsed_url.hostname] = set()
            self.subdomains[parsed_url.hostname].add(fragmentless_url)

            # Check if the url is allowed by robots.txt
            if not parser.can_fetch(self.config.user_agent, url): 
                return

            self.save[urlhash] = (fragmentless_url, scraped)
            self.save.sync()
            self.domains_to_scrape[domain].put(url)

    def is_similar(self, key, simhash):
        """
        Checks if the given simhash is similar to any already seen simhashes.

        Parameters:
            simhash (Simhash): Simhash to compare
        Returns:
            bool: True if similar, False otherwise
        """
        with self.shi_lock:
            similar =  bool(self.simhash_index.get_near_dups(simhash))
            if similar is True:
                self.add_simhash(key, simhash)
            return similar
    
    def add_simhash(self, key, simhash):
        """
        Add simhash to simhash index.

        Parameters:
            simhash (Simhash): Simhash to add
        """
        with self.shi_lock:
            self.simhash_index.add(key, simhash)

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
    
    def add_words(self, words: Counter, url: str):
        """
        Add words to word count and update max words if necessary.

        Parameters:
            words (Counter): words to add to word count
        """
        with self.lock:
            self._update_max_words(sum(words.values()), url)
            self.word_count += words
    
    def _update_max_words(self, count, url):
        """
        Update max words for frontier set.

        Parameters:
            url (str): url to add to frontier set
        """
        with self.lock:
            if count > self.max_words[1]:
                self.max_words = (url, count)
    
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
        with self.lock:
            urlhash = get_urlhash(robots_url)
            if urlhash not in self.save:
                self.save[urlhash] = (robots_url, True)
                self.save.sync()

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
                sitemap_urls.append(link)
        
        self.process_sitemaps(sitemap_urls)

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
        namespace = root.tag.split('}')[0] + '}'
        # Check if it's a sitemap index
        is_sitemap_index = False
        for sitemap_index in root.findall(namespace + "sitemap"):
            is_sitemap_index = True
            loc_element = sitemap_index.find(namespace + "loc")
            if loc_element is not None:
                sitemap_urls = self.get_urls_from_sitemap(loc_element.text)
                urls.extend(sitemap_urls)

        # If it's not a sitemap index, it's a regular sitemap
        if not is_sitemap_index:
            for url_element in root.findall(namespace + "url"):
                loc_element = url_element.find(namespace + "loc")
                if loc_element is not None:
                    urls.append(loc_element.text)

        return urls
    
    def handle_shelves(self, restart):
        """
        Handle shelves, delete them if restarting, create them if not restarting.

        Parameters:
            restart (bool): whether or not to restart
        """
        if restart:
            if os.path.exists(self.config.save_file + '.db'):
                self.logger.info(
                    f"Found save file {self.config.save_file}, deleting it.")
                os.remove('' + self.config.save_file + '.db')
            
            if os.path.exists(self.config.save_file + '_data' + '.db'):
                self.logger.info(
                    f"Found data save file {self.config.save_file + '_data'}, deleting it.")
                os.remove('' + self.config.save_file + '_data' + '.db')

        else:
            if not os.path.exists(self.config.save_file + '.db'):
                self.logger.info(
                    f"Did not find save file {self.config.save_file}, "
                    f"starting from seed.")
       
        
            if not os.path.exists(self.config.save_file + '_data' + '.db'):
                self.logger.info(
                    f"Did not find data save file {self.config.save_file + '_data'}, "
                    f"starting fresh.")
            else:
                self.load_data()
        
        self.save = shelve.open(self.config.save_file)
        self.save_data()
    
    def maybe_save_data(self):
        """
        Save data if the number of processed links has reached the save_interval threshold.
        """
        self.links_processed += 1
        if self.links_processed % self.save_interval == 0:
            self.logger.info(f"Saving data after processing {self.links_processed} links.")
            self.save_data()
        
    def save_data(self):
        """
        Save data structures to disk.
        """
        with self.lock:
            with shelve.open(self.config.save_file + '_data') as data_file:
                data_file['subdomains'] = self.subdomains
                data_file['word_count'] = self.word_count
                data_file['max_words'] = self.max_words
    
    def load_data(self):
        """
        Load data structures from disk.
        """
        with self.lock:
            with shelve.open(self.config.save_file + '_data') as data_file:
                self.subdomains = data_file['subdomains']
                self.word_count = data_file['word_count']
                self.max_words = data_file['max_words']


    def __del__(self):
        """
        Destructor for Frontier class.
        """
        self.save.close()
        self.save_data()


