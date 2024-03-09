from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        """
        This class implements a crawler that uses a frontier and a set of workers to
        download webpages and extract links to other pages.

        Args:
            config (obj): The configuration object that contains the settings for the
                crawler.
            restart (bool): A flag indicating whether to restart the crawler from the
                beginning or to resume from the last checkpoint.
            frontier_factory (func, optional): A function that creates an instance of the
                frontier class. The default is Frontier.
            worker_factory (func, optional): A function that creates an instance of the
                worker class. The default is Worker.

        Attributes:
            config (obj): The configuration object that contains the settings for the
                crawler.
            logger (Logger): A logger instance for logging messages.
            frontier (Frontier): The frontier object that stores and manages the URLs to
                be crawled.
            workers (list): A list of worker threads that process URLs from the frontier.
            worker_factory (func): A function that creates an instance of the worker
                class.

        """
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        """
        Starts the crawler in asynchronous mode.

        This function starts a number of worker threads that process URLs from the frontier.
        The function returns immediately and the worker threads run in the background.

        Args:
            None

        Returns:
            None

        """
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        """
        Start the crawler.

        This function starts the crawling process by starting a number of worker threads,
        which in turn process URLs from the frontier. The function blocks until all worker
        threads have completed their tasks or until an error occurs.

        Returns:
            None

        """
        self.start_async()
        self.join()

    def join(self):
        """
        Join all worker threads.

        This function blocks until all worker threads have completed their tasks or until an
        error occurs.

        Returns:
            None

        """
        for worker in self.workers:
            worker.join()
