import re


class Config(object):
    """
    A class representing the crawler configuration.
    
    Attributes:
        user_agent (str): The user agent string for the crawler.
        threads_count (int): The number of worker threads.
        save_file (str): The filename for saving the crawler state.
        host (str): The host for the cache server.
        port (int): The port number for the cache server.
        seed_urls (list): The list of seed URLs for the crawler.
        time_delay (float): The politeness time delay between requests.
        cache_server (str): The cache server address (host and port).
    """
    def __init__(self, config):
        self.user_agent = config["IDENTIFICATION"]["USERAGENT"].strip()
        print (self.user_agent)
        assert self.user_agent != "DEFAULT AGENT", "Set useragent in config.ini"
        assert re.match(r"^[a-zA-Z0-9_ ,]+$", self.user_agent), "User agent should not have any special characters outside '_', ',' and 'space'"
        self.threads_count = int(config["LOCAL PROPERTIES"]["THREADCOUNT"])
        self.save_file = config["LOCAL PROPERTIES"]["SAVE"]

        self.host = config["CONNECTION"]["HOST"]
        self.port = int(config["CONNECTION"]["PORT"])

        self.seed_urls = config["CRAWLER"]["SEEDURL"].split(",")
        self.time_delay = float(config["CRAWLER"]["POLITENESS"])

        self.cache_server = None