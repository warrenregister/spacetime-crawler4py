import pickle

class Response(object):
    """
    A class representing an HTTP response.

    Attributes:
        url (str): The requested URL.
        status (int): The HTTP status code.
        error (str): An error message, if any.
        raw_response (bytes): The raw response content.
    """
    def __init__(self, resp_dict):
        self.url = resp_dict["url"]
        self.status = resp_dict["status"]
        self.error = resp_dict["error"] if "error" in resp_dict else None
        try:
            self.raw_response = (
                pickle.loads(resp_dict["response"])
                if "response" in resp_dict else
                None)
        except TypeError:
            self.raw_response = None
