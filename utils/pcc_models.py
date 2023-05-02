from rtypes import pcc_set, dimension, primarykey


@pcc_set
class Register(object):
    """
    A class representing a crawler registration with the load balancer.

    Attributes:
        crawler_id (str): A unique identifier for the crawler.
        load_balancer (tuple): The address (host and port) of the load balancer.
        fresh (bool): Indicates if the registration is new.
        invalid (bool): Indicates if the registration is invalid.
    """
    crawler_id = primarykey(str)
    load_balancer = dimension(tuple)
    fresh = dimension(bool)
    invalid = dimension(bool)

    def __init__(self, crawler_id, fresh):
        self.crawler_id = crawler_id
        self.load_balancer = tuple()
        self.fresh = fresh
        self.invalid = False
