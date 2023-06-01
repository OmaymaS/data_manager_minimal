import json
import logging

from apache_beam.coders import Coder

logging.getLogger().setLevel(logging.INFO)


class JsonCoder(Coder):
    """
    A JSON coder interpreting each line as a JSON string.
    """

    def encode(self, x):
        return json.dumps(x).encode("utf-8")

    def decode(self, x):
        return json.loads(x)
