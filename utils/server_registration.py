import os
from spacetime import Node
from utils.pcc_models import Register

def init(df, user_agent, fresh):
    """
    Initialize the cache server connection.

    Args:
        df (Dataframe): A Spacetime Dataframe object.
        user_agent (str): The user agent string for the crawler.
        fresh (bool): Indicates if the cache server should be started fresh.

    Returns:
        tuple: The address (host and port) of the cache server.
    """

    reg = df.read_one(Register, user_agent)
    if not reg:
        reg = Register(user_agent, fresh)
        df.add_one(Register, reg)
        df.commit()
        df.push_await()
    while not reg.load_balancer:
        df.pull_await()
        if reg.invalid:
            raise RuntimeError("User agent string is not acceptable.")
        if reg.load_balancer:
            df.delete_one(Register, reg)
            df.commit()
            df.push()
    return reg.load_balancer

def get_cache_server(config, restart):
    """
    Initialize the Spacetime Node and retrieve the cache server address (host and port) as part of the initialization process.

    Args:
        config (Config): A Config object containing the crawler configuration.
        restart (bool): Indicates if the cache server should be restarted.

    Returns:
        tuple: The address (host and port) of the cache server obtained during the Spacetime Node initialization.
    """
    init_node = Node(
        init, Types=[Register], dataframe=(config.host, config.port))
    return init_node.start(
        config.user_agent, restart or not os.path.exists(config.save_file))