class Request_info(object):
    """A data stream from any platform/hub:
    """

    def __init__(self, api_core_url, users_feed_name, feed_info):
        self.last_fetch_time = None

        self.api_core_url = api_core_url
        self.users_feed_name = users_feed_name
        self.feed_info = feed_info
