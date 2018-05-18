from request_info_list import Request_info_list
from request_info_restful_bt_fetch import Request_info_restful_bt_fetch
from request_info_restful_cdp_fetch import Request_info_restful_cdp_fetch
from request_info_osisoft_pi_fetch import Request_info_osisoft_pi_fetch, Feed_type_pi, Request_type_pi
from enum import Enum

import json


HUB_API_BT = 'http://api.bt-hypercat.com'
HUB_API_CDP = 'https://api.cityverve.org.uk/v1'
HUB_API_TRIANGULUM = 'https://130.88.97.137/piwebapi'


class Data_request_type(Enum):
    feed_metadata = 1
    stream_metadata = 2
    data = 3


class Request_info_fetch_list(Request_info_list):
    """A data stream from any platform/hub:
    """

    def __init__(self):
        super(Request_info_fetch_list, self).__init__()

    def clear_all(self):
        self.requests = []

    def append_request(self, request_params, api_key=None, username=None):
        try:
            hub_api = request_params['stream_params'][0]
        except:
            hub_api = request_params.split(',')[0]

        if (hub_api == HUB_API_BT):
            self.append(Request_info_restful_bt_fetch(username, api_key, request_params))
        elif (hub_api == HUB_API_TRIANGULUM):
            self.append(Request_info_osisoft_pi_fetch(request_params))
        elif (hub_api == HUB_API_CDP):
            self.append(Request_info_restful_cdp_fetch(api_key, request_params))
        else:
            raise ValueError("Unknown hub api as first value in 'stream_params' list: " + hub_api)


    def append_request_list(self, request_params_list, api_key=None, username=None):
        # import hypercat streams
        for line in request_params_list:
            self.append_request(line, api_key, username)


    def append(self, stream):
        self.requests.append(stream)

    def get_list_of_users_stream_ids(self):
        result = []

        for request in self.requests:
            result.append(request.users_feed_name)

        return result









