from requestInfoList import RequestInfoList
from requestInfoFetchFactory import RequestInfoFetchFactory
from enum import Enum

import json

class Data_request_type(Enum):
    feed_metadata = 1
    stream_metadata = 2
    data = 3


class RequestInfoFetchList(RequestInfoList):
    """A data stream from any platform/hub:
    """

    def __init__(self):
        super(RequestInfoFetchList, self).__init__()

    def clear_all(self):
        self.requests = []

    def append_request(self, hub_short_title, request_params, api_key=None, username=None):
        """try:
            hub_api = request_params['stream_params'][0]
        except:
            hub_api = request_params.split(',')[0]"""

        self.append(RequestInfoFetchFactory.create_request_info_fetch(hub_short_title, request_params, username, api_key))

        """if (hub_api == HUB_API_BT):
            self.append(Request_info_restful_bt_fetch(username, api_key, request_params))
        elif (hub_api == HUB_API_TRIANGULUM):
            self.append(Request_info_osisoft_pi_fetch(request_params))
        elif (hub_api == HUB_API_CDP):
            self.append(Request_info_restful_cdp_fetch(api_key, request_params))
        else:
            raise ValueError("Unknown hub api as first value in 'stream_params' list: " + hub_api)"""


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









