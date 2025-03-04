import numpy as np
from collections import Counter

class SimHash:
    def __init__(self, content, bits=64):
        self.num_bits = bits
        self.hash = self.simhash(content)

    def simhash(self, counter: Counter):
        """
        Calculates the Simhash value for a given set of tokens and their weights.

        Args:
            counter (Counter): A Counter object containing the tokens and their weights.

        Returns:
            int: The Simhash value.
        """
        weights = [0] * self.num_bits  # vector of 0s to hold the simhash
        for t in counter.keys():
            h = hash(t)  # get hash of token
            for i in range(self.num_bits):
                bitmask = 1 << i  # if the ith bit is 1 add the weight for the token
                if h & bitmask:
                    weights[i] += counter[t]
                else:
                    weights[i] -= counter[t]
        fingerprint = 0
        # calc fingerprint
        for i in range(self.num_bits):
            if weights[i] >= 0:
                fingerprint += 1 << i
        return fingerprint

    def similarity(self, other_hash):
        """
        Calculates the similarity between two SimHash objects.

        Args:
            other_hash (SimHash): The other SimHash object.

        Returns:
            float: The similarity between the two objects. A value of 1 indicates a perfect match, while a value of 0 indicates no similarity.
        """
        # get different bits
        x = (self.hash ^ other_hash.hash) & ((1 << self.num_bits) - 1)
        # return proportion of different bits
        return (self.num_bits - bin(x).count('1')) / float(self.num_bits)