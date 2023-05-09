from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
import nltk
import multiprocessing



def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()
    crawler.frontier.save.close()
    crawler.frontier.pickle_fields()


if __name__ == "__main__":
    multiprocessing.set_start_method('fork')

    nltk.download('stopwords')
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=True)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
