from request_info_restful_cdp import Request_info_restful_cdp, Element_type
from enum import Enum
import json

#E.G.: https://api.cityverve.org.uk/v1/entity/noise-meter/6472W/timeseries/dose/datapoints

class Stream_type(Enum):
    __order__ = 'timeseries'
    none = 0
    timeseries = 1
    static = 2

class Request_info_restful_cdp_fetch(Request_info_restful_cdp):
    """A data stream from the BT Data Hub

    """
    @staticmethod
    def get_stream_types():
        return [(e.value, e.name) for e in Stream_type]

    def __init__(self, api_key, api_core_url, hub_version, element_type, element_id, instance_id,
                 stream_type, stream_id, datapoints, params, users_feed_name, feed_info):
        super(Request_info_restful_cdp_fetch, self).__init__(api_key,
                                                             api_core_url,
                                                             hub_version,
                                                             element_type,
                                                             element_id,
                                                             instance_id,
                                                             users_feed_name,
                                                             feed_info)
        self.stream_type = stream_type      # timeseries
        self.stream_id = stream_id          # dose
        self.datapoints = datapoints        # datapoints
        self.params = params                # API's allowed param list eg 'offset=12&limit=10'

    def url_string(self):

        result = self.api_core_url + '/' + self.hub_version + '/' + Element_type(self.element_type).name
        if self.element_id.strip() != '':
            result += '/'+ self.element_id
            if self.instance_id.strip() != '':
                result += '/' + self.instance_id
                if Stream_type(self.stream_type).value == 1: # == timeseries
                    result += "/" + Stream_type(self.stream_type).name
                    if self.stream_id.strip() != '':
                        result += '/' + self.stream_id
                        if self.datapoints == True:
                            result += '/datapoints'
        return result


    def user_defined_name(self):
        return self.users_feed_name