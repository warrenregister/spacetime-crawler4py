import os
import shelve
import time
from collections import Counter, defaultdict
from queue import Empty, Queue
from threading import RLock
from urllib.parse import urljoin, urlparse
from io import StringIO
from lxml import etree
from scraper import is_valid
from utils.download import download
from utils import get_logger, get_urlhash, normalize
from crawler.robot_parser import CustomRobotsParser
import pickle

# Remember to not visit the cache website yet
class Frontier(object):
    """
    Frontier class for frontier set and frontier queue.

    Attributes:
        politeness_delay (float): delay between requests to same domain
        domains (dict): dict of Queue for each domain cond
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
        self.simhash_lock = RLock() # lock for simhash dictionary
        self.sitemaps_lock = RLock() # lock for sitemaps
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.simhashes = {}
        self.low_data_urls = set()
        self.error_urls = set()
        self.links_processed = 0
        self.backup_interval = 300  # Backup interval in seconds (e.g., 1200 seconds = 20 minutes)
        self.backups = './backup_datastructures'  # Folder to store backups
        self.last_backup_time = time.time()  # Initialize last backup time

        self.handle_shelves(restart)

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url, 0)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url, 0)

    def _parse_save_file(self):
        """
        Parse save file and add urls to frontier.
        """
        with self.lock:
            total_count = len(self.save)
            tbd_count = 0
            for url, depth, completed in self.save.values():
                if not completed and is_valid(url):
                    domain = urlparse(url).hostname
                    self.domains_to_scrape[domain].put((url, depth))
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
            self.pickle_fields()
            while len(self.domains_to_scrape) > 0:
                for domain, url_queue in list(self.domains_to_scrape.items()):
                    if domain in self.last_request_time:
                        last_request_time = self.last_request_time[domain]
                    else:
                        last_request_time = 0
                    time_since_last_request = time.time() - last_request_time

                    if time_since_last_request >= self.politeness_delay:
                        try:
                            url, depth = url_queue.get(block=False)
                            self.last_request_time[domain] = time.time()
                            return url, depth
                        except Empty:
                            del self.domains_to_scrape[domain]
                time.sleep(self.politeness_delay)
            
            return None, None
    
    def add_url(self, url, depth, scraped=False):
        """
        Add url to frontier.

        Parameters:
            url (str): url to add to fron, tier
            depth (int): depth of url
            scraped (bool): whether url has been scraped
        """
        url = normalize(url)

        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        fragmentless_url = parsed_url._replace(fragment="").geturl()
        urlhash = get_urlhash(fragmentless_url)


        with self.lock and self.robots_parsers_lock:
            # Check if the url is already in the frontier
            if urlhash in self.save:
                self.logger.info(f"URL {url} already in frontier.")
                return
            # Check if the domain is new -> fetch robots.txt and process sitemaps
            if domain not in self.robots_parsers.keys():
                parser = self.get_robots_txt_parser(url)
                self.get_sitemap_urls_from_robots_txt(url)  
            else:
                parser = self.robots_parsers[domain]
            
            # add url to subdomains
            if parsed_url.hostname not in self.subdomains.keys():
                self.subdomains[parsed_url.hostname] = set()
            self.subdomains[parsed_url.hostname].add(fragmentless_url)

            # Check if the url is allowed by robots.txt
            if not parser.can_fetch(parsed_url.path): 
                self.logger.info(f"URL path of {url} not allowed by robots.txt.")
                return

            self.save[urlhash] = (fragmentless_url, depth, scraped)
            self.save.sync()
            self.domains_to_scrape[domain].put((fragmentless_url, depth))

    def get_simhashes(self):
        """
        Get simhashes.
        """
        with self.simhash_lock:
            return self.simhashes.copy()
            
    def add_simhash(self, url, simhash):
        """
        Add simhash to simhash index.

        Parameters:
            url (str): url to add
            simhash (Simhash): Simhash to add
        """
        with self.simhash_lock:
            self.simhashes[simhash] = url
    
    def get_bad_urls(self):
        """
        Get both low data urls and error urls.
        """
        with self.lock:
            return self.low_data_urls.copy(), self.error_urls.copy()

    def add_low_data_url(self, url):
        """
        Add low data url to low data url index.

        Parameters:
            url (str): url to add
        """
        with self.lock:
            self.low_data_urls.add(url)
    
    def add_error_url(self, url, status):
        """
        Add error url to error url index.

        Parameters:
            url (str): url to add
            status (int): status code of error
        """
        with self.lock:
            if status >= 400:
                self.error_urls.add(url)

    def mark_url_complete(self, url, depth):
        """
        Mark url as completed.

        Parameters:
            url (str): url to mark as completed
            depth (int): depth of url
        """
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, depth, True)
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
            # use pickle to save counter
            with open('word_count.pkl', 'wb') as f:
                pickle.dump(self.word_count, f)
    
    def _update_max_words(self, count, url):
        """
        Update max words for frontier set.

        Parameters:
            url (str): url to add to frontier set
        """
        with self.lock:
            if count > self.max_words[1]:
                self.max_words = (url, count)
                # use pickle to save max words
                with open('max_words.pkl', 'wb') as f:
                    pickle.dump(self.max_words, f)
    
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
                self.save[urlhash] = (robots_url, 1, True)
                self.save.sync()

            # Wait for politeness delay if necessary
            if domain in self.last_request_time:
                time_since_last_request = time.time() - self.last_request_time[domain]
                if time_since_last_request < self.config.politeness_delay:
                    time.sleep(self.config.politeness_delay - time_since_last_request)
            self.last_request_time[domain] = time.time()
            
            try:
                resp = download(robots_url, self.config, self.logger)
            except Exception as e:
                self.logger.error(f"Error getting robots.txt from {domain}: {e}")
                # Return an empty robots parser if there's an error
                return CustomRobotsParser(self.config.user_agent)  # empty parser

            # Check if the content type is text/plain
            if resp.status == 200 and resp.raw_response is not None and \
                resp.raw_response.headers.get('Content-Type', '').startswith('text/plain'):
                robots_txt_content = resp.raw_response.content.decode("utf-8").replace('\r\n', '\n')
                parser = CustomRobotsParser(self.config.user_agent)
                parser.parse(robots_txt_content)
            else:
                self.logger.error(f"Error getting robots.txt from {domain}")
                # Return an empty robots parser if there's an error
                parser = CustomRobotsParser(self.config.user_agent)  # empty parser

        return parser

    def get_sitemap_urls_from_robots_txt(self, url):
        """
        Get sitemap urls from robots.txt for domain of url.

        Parameters:
            url (str): url to get sitemap urls for
        
        Returns:
            list: list of sitemap urls
        """
        url = normalize(url)
        domain = urlparse(url).netloc
        with self.robots_parsers_lock and self.sitemaps_lock:
            if domain in self.robots_parsers.keys():
                parser = self.robots_parsers[domain]
            else:
                parser = self.get_robots_txt_parser(url)
            
            sitemap_urls = []
            for link in parser.get_sitemaps():
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
            sitemap_url = normalize(sitemap_url)
            domain = urlparse(sitemap_url).netloc
            with self.sitemaps_lock:
                if domain not in self.sitemaps:
                    self.sitemaps[domain] = set()
                if sitemap_url not in self.sitemaps[domain]:
                    self.sitemaps[domain].add(sitemap_url)

                    urls_from_sitemap = self.get_urls_from_sitemap(sitemap_url)
                    for url in urls_from_sitemap:
                        if is_valid(url):
                            self.add_url(url, 1)
                    

    def get_urls_from_sitemap(self, sitemap_url):
        """
        Get urls from sitemap.

        Parameters:
            sitemap_url (str): url of sitemap
        Returns:
            list: list of urls from sitemap
        """

        if sitemap_url in self.last_request_time:
            time_diff = time.time() - self.last_request_time[sitemap_url]
            if time_diff < self.politeness_delay:
                time.sleep(time_diff)
        
        # respect politeness if necessary
        domain = urlparse(sitemap_url).netloc
        if domain in self.last_request_time:
            time_since_last_request = time.time() - self.last_request_time[domain]
            if time_since_last_request < self.politeness_delay:
                time.sleep(self.politeness_delay - time_since_last_request)
        
        try:
            resp = download(sitemap_url, self.config, self.logger)
        except Exception as e:
            self.logger.error(f"Error {e} downloading sitemap {sitemap_url}")
            return []

        if resp.status != 200:
            self.logger.error(f"Error w/ status {resp.status} downloading sitemap {sitemap_url}")
            return []

        try:
            root = etree.fromstring(resp.raw_response.content)
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
            if os.path.exists(self.backups + '/' + self.config.save_file + '.db'):
                self.logger.info(
                    f"Found save file {self.config.save_file}, deleting it.")
                os.remove(self.backups + '/' + self.config.save_file + '.db')
        else:
            if not os.path.exists(self.backups + '/' + self.config.save_file + '.db'):
                self.logger.info(
                    f"Did not find save file {self.backups + '/' + self.config.save_file}, "
                    f"starting from seed.")

        with self.lock:
            self.save = shelve.open(self.backups + '/' + self.config.save_file)
            # check if pickle file exists, if so, load it,
            for fname, attr in [('subdomains.pkl', defaultdict(set)),
                                ('word_count.pkl', Counter()),
                                ('max_words.pkl', (None, 0)),
                                ('robots_parsers.pkl', {}),
                                ('sitemaps.pkl', {}), 
                                ('simhashes.pkl', {}),
                                ('last_request_time.pkl', {}),
                                ('last_backup_time.pkl', 0),
                                ('bad_urls.pkl', set()),
                                ('errors.pkl', set()),]:
                path = os.path.join(self.backups, fname)
                if os.path.exists(path) and not restart:
                    with open(path, 'rb') as f:
                        setattr(self, fname[:-4], pickle.load(f))
                else:
                    setattr(self, fname[:-4], attr)

            self.save.sync()
    
    def pickle_fields(self):
        """
        Pickle fields.
        """
        current_time = time.time()
        if current_time - self.last_backup_time > self.backup_interval:
            with self.lock and self.robots_parsers_lock and self.sitemaps_lock and self.simhash_lock:
                with open(self.backups + '/subdomains.pkl', 'wb') as f:
                    pickle.dump(self.subdomains, f)
                with open(self.backups + '/word_count.pkl', 'wb') as f:
                    pickle.dump(self.word_count, f)
                with open(self.backups + '/max_words.pkl', 'wb') as f:
                    pickle.dump(self.max_words, f)
                with open(self.backups + '/robots_parsers.pkl', 'wb') as f:
                    pickle.dump(self.robots_parsers, f)
                with open(self.backups + '/sitemaps.pkl', 'wb') as f:
                    pickle.dump(self.sitemaps, f)
                with open(self.backups + '/simhashes.pkl', 'wb') as f:
                    pickle.dump(self.simhashes, f)
                with open(self.backups + '/last_request_time.pkl', 'wb') as f:
                    pickle.dump(self.last_request_time, f)
                with open(self.backups + '/bad_urls.pkl', 'wb') as f:
                    pickle.dump(self.bad_urls, f)
                with open(self.backups + '/errors.pkl', 'wb') as f:
                    pickle.dump(self.errors, f)
                self.last_backup_time = current_time

    def __del__(self):
        """
        Destructor for Frontier class.
        """
        if self.save is not None:
            self.save.close()
        self.pickle_fields()

