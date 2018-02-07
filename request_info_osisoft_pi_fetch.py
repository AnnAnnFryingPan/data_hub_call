from request_info_osisoft_pi import Request_info_osisoft_pi, Feed_type_pi, Request_type_pi
import json

class Request_info_osisoft_pi_fetch(Request_info_osisoft_pi):
    """A data stream from any hypercat platform/hub:
    """

    def __init__(self, host, url, feed_type, params, request_type, users_feed_name, feed_info):
        super(Request_info_osisoft_pi_fetch, self).__init__(host, url, feed_type, params, request_type,
                                                            users_feed_name, feed_info)

    def url_string(self):
        result = self.api_core_url

        if(Feed_type_pi(self.feed_type).value > 0):
            result +=  '/' + Feed_type_pi(self.feed_type).name
        if(len(self.params) > 0):
            result += '/' + self.params
        if (Request_type_pi(self.request_type).value > 0):
            result += "/" + Request_type_pi(self.request_type).name
        return result


    def csv_line_string(self, includeUsersName=True, includesFeedInfo=True):
        # https://130.88.97.137/piwebapi,streams/
        # A0EXpbRmwnc7kq0OSy1LydJJQxey0y1BT5hGA3gBQVqtCQgLR6A7xNbjk6RXQ6dns2VqAVk0tUEktUDAxLkRTLk1BTi5BQy5VS1x
        # UUklBTkdVTFVNXEFJUiBRVUFMSVRZXE9YRk9SRCBST0FEfE5PIExFVkVM/interpolated,Anns_Pi_feed_Nitrogen

        result = self.api_core_url
        result += ',' + Feed_type_pi(self.feed_type).name
        result += ',' + self.params
        result += "," + self.request_type.name
        if(includeUsersName):
            result += "," + self.users_feed_name
        if (includesFeedInfo):
            result += "," + json.dumps(self.feed_info)

        return result

