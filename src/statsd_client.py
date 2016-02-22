from socket import socket, AF_INET, SOCK_DGRAM

class StatsdClient(object):
    SC_COUNT = "c"

    def __init__(self, host='localhost', port=8125):
        """
        Sends statistics to the stats daemon over UDP

        >>> from python_example import StatsdClient
        """
        self.addr = (host, port)

    def count(self, stats, value, sample_rate=1):
        """
        Updates one or more stats counters by arbitrary value

        >>> client = StatsdClient()
        >>> client.count('example.counter', 17)
        """
        self.update_stats(stats, value, self.SC_COUNT, sample_rate)

    def update_stats(self, stats, value, _type, sample_rate=1):
        """
        Pipeline function that formats data, samples it and passes to send()

        >>> client = StatsdClient()
        >>> client.update_stats('example.update_stats', 73, "c", 0.9)
        """
        stats = self.format(stats, value, _type)
        self.send(stats, self.addr)

    @staticmethod
    def format(keys, value, _type):
        """
        General format function.

        >>> StatsdClient.format("example.format", 2, "T")
        {'example.format': '2|T'}
        >>> formatted = StatsdClient.format(("example.format31", "example.format37"), "2", "T")
        >>> formatted['example.format31'] == '2|T'
        True
        >>> formatted['example.format37'] == '2|T'
        True
        >>> len(formatted)
        2
        """
        data = {}
        value = "{0}|{1}".format(value, _type)
        # TODO: Allow any iterable except strings
        if not isinstance(keys, (list, tuple)):
            keys = [keys]
        for key in keys:
            data[key] = value
        return data

    @staticmethod
    def send(_dict, addr):
        """
        Sends key/value pairs via UDP.

        >>> StatsdClient.send({"example.send":"11|c"}, ("127.0.0.1", 8125))
        """
        # TODO(rbtz@): IPv6 support
        # TODO(rbtz@): Creating socket on each send is a waste of recources
        udp_sock = socket(AF_INET, SOCK_DGRAM)
        # TODO(rbtz@): Add batch support
        for item in _dict.items():
            udp_sock.sendto(":".join(item).encode('utf-8'), addr)
