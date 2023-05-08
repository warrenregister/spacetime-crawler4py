from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import re


class Worker(Thread):
    """
    The Worker class is a subclass of Thread that downloads URLs from the Frontier
    and processes them using the scraper function from scraper.py. Each Worker
    instance runs concurrently in a separate thread.
    
    Attributes:
        config (Config): A Config object containing the crawler configuration.
        frontier (Frontier): A Frontier object managing the list of URLs to be downloaded and their state.
    """

    def __init__(self, worker_id, config, frontier):
        """
        Initialize the Worker with the given worker_id, configuration, and frontier.
        
        Args:
            worker_id (int): A unique identifier for the worker.
            config (Config): A Config object containing the crawler configuration.
            frontier (Frontier): A Frontier object managing the list of URLs to be downloaded and their state.
        """
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
    
    def run(self):
        """
        Run the worker loop until there are no more URLs to be downloaded.
        """
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if tbd_url is None:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            if self.is_infinite_trap(tbd_url):
                self.logger.info(f"Skipping {tbd_url}, infinite trap detected.")
                self.frontier.mark_url_complete(tbd_url)
                continue

            try:
                resp = download(tbd_url, self.config, self.logger)
            except Exception as e:
                self.logger.error(f"Error while downloading {tbd_url}: {str(e)}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # check if resp is a redirect or error
            if 300 <= resp.status <= 399:
                self.logger.info(f"Skipping {tbd_url}, status <{resp.status}>.")
                redirect_url = resp.raw_response.headers.get('Location')
                if redirect_url is not None:
                    self.logger.info(f"Redirected {tbd_url} to {redirect_url}.")
                    self.frontier.add_url(redirect_url)
                self.frontier.mark_url_complete(tbd_url)
                continue
            elif resp.status != 200:
                self.logger.info(f"Skipping {tbd_url}, status <{resp.status}>.")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # check if resp.raw_response is None
            if resp.raw_response is None:
                self.logger.info(f"Skipping {tbd_url}, empty raw_response.")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # scrape the resp
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            try:
                scraped_urls, words, simhash = scraper.scraper(tbd_url, resp)
            except Exception as e:
                self.logger.error(f"Error while scraping {tbd_url}: {str(e)}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            similar = False
            if simhash is not None:
                similar = self.frontier.is_similar(tbd_url, simhash)
            if similar:
                self.logger.info(f"Skipping {tbd_url}, similar content.")
            
            # add scraped words to frontier
            if words is not None and not similar:
                self.frontier.add_words(words, tbd_url)
            
            if not similar:
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
    

        @staticmethod
        def is_infinite_trap(url):
            trap_patterns = [
                # Repetitive patterns
                r'(\b\w+\b).*\1',
                
                # Calendars
                r'\b(19[0-9]{2}|2[0-9]{3})/(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])\b',
                
                # Checkout and account related
                r'\b(admin|cart|checkout|favorite|password|register|sendfriend|wishlist)\b',
                
                # Script related
                r'\b(cgi-bin|includes|var)\b',
                
                # Ordering and filtering related
                r'\b(filter|limit|order|sort)\b',
                
                # Session related
                r'\b(sessionid|session_id|SID|PHPSESSID)\b',
                
                # Other
                r'\b(ajax|cat|catalog|dir|mode|profile|search|id|pageid|page_id|docid|doc_id)\b',
                
                # Social media sites
                r'\b(?:twitter\.com|www\.twitter\.com|facebook\.com|www\.facebook\.com|tiktok\.com|www\.tiktok\.com|instagram\.com|www\.instagram\.com)\b',
            ]

            for pattern in trap_patterns:
                if re.search(pattern, url):
                    return True
            return False
