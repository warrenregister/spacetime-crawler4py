from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


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
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.frontier.register_request(tbd_url)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls, words, subdomain = scraper.scraper(tbd_url, resp)
            self.frontier.add_words(words)
            self.frontier.add_subdomain(subdomain)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
