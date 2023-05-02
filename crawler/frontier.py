import os
import shelve
from collections import Counter

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    """
    The Frontier class manages the list of URLs to be downloaded and the 
    state of downloaded URLs. It interacts with a save file to store and 
    retrieve the state of the crawling process.
    
    Attributes:
        config (Config): A Config object containing the crawler configuration.
        save (Shelve): A shelve object for storing and retrieving the state of the crawling process.
        to_be_downloaded (list): A list of URLs that have not been downloaded yet.
    """
    def __init__(self, config, restart):
        """
        Initialize the Frontier with the given configuration and restart flag.
        
        Args:
            config (Config): A Config object containing the crawler configuration.
            restart (bool): A flag indicating whether to restart the crawling process from the seed URLs.
        """
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.word_count = Counter()
        self.domain_rules = {}
        self.subdomains = set()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        """
        Parse the save file to initialize the state of the frontier. 
        This method can be overridden for alternate saving techniques.
        """
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        """
        Get one URL that has to be downloaded from the list of to_be_downloaded URLs.
        
        Returns:
            str: A URL to be downloaded or None if there are no more URLs.
        """
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url):
        """
        Add a new URL to the frontier to be downloaded later. The URL will be 
        normalized and its hash will be checked to prevent adding duplicate URLs.
        
        Args:
            url (str): The URL to be added to the frontier.
        """
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        """
        Mark a URL as completed in the save file so that it won't be downloaded again on restart.
        
        Args:
            url (str): The URL to be marked as completed.
        """
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
    

    def add_words(self, words: Counter):
        """
        Add words to the word count.
        
        Args:
            words (Counter): A Counter object containing the words to be added.
        """
        self.word_count += words
    

    def add_domain_rule(self, domain: str, rule):
        """
        Add a domain rule to the domain_rules dictionary.
        
        Args:
            domain (str): The domain to be added.
            rule (str): The rule to be added.
        """
        self.domain_rules[domain] = rule
    

    def add_subdomain(self, subdomain: str):
        """
        Add a subdomain to the subdomains set.
        
        Args:
            subdomain (str): The subdomain to be added.
        """
        self.subdomains.add(subdomain)
