from request_info_restful_bt import Request_info_restful_bt, Feed_type, Request_type
from enum import Enum
import json


class Request_type_ds_or_features(Enum):
    none = 0
    datastreams = 1
    features = 2


class Request_info_restful_cdp_fetch(Request_info_restful_bt):
    """A data stream from the BT Data Hub

    Attributes:
        api_core_url: The url of the data hub. eg 'http://api.bt-hypercat.com'
        feed_id: The id of the parent feed to which the datastream belongs
        datastream_id: the id of the datastream. Eg. 0, 1, 2...
        feed_type: either 'sensors', 'events', 'locations' or 'geo'
    """

    @staticmethod
    def get_request_type_ds_or_features():
        return [(e.value, e.name) for e in Request_type_ds_or_features]

    def __init__(self, api_key, username, api_core_url, feed_type, feed_id, request_type_ds_or_feature, datastream_id,
                 request_type, params, users_feed_name, feed_info):
        super(Request_info_restful_bt_fetch, self).__init__(api_key, username,
                                                         api_core_url,
                                                         feed_type,
                                                         feed_id,
                                                         datastream_id,
                                                         request_type,
                                                         users_feed_name,
                                                         feed_info)
        self.request_type_ds_or_feature = request_type_ds_or_feature
        self.params = params


    def url_string(self):
        result = self.api_core_url + '/' + Feed_type(self.feed_type).name + '/feeds/' + self.feed_id

        if (Request_type_ds_or_features(self.request_type_ds_or_feature).value > 0):
            result += "/" + self.request_type_ds_or_feature.name
        if (self.datastream_id > -1):
            result += "/" + str(self.datastream_id)
        if (Request_type(self.request_type).value > 0):
            result += "/" + self.request_type.name

        return result

    def csv_line_string(self, includeUsersName=True, includesFeedInfo=True):
        # http://api.bt-hypercat.com,sensors,86a25d4e-25fc-4ebf-a00d-0a603858c7e1,datastreams,0,datapoints,{},Anns_carpark_stream

        result = self.api_core_url
        result += ',' + Feed_type(self.feed_type).name
        result += ',' + self.feed_id
        result += "," + self.request_type_ds_or_feature.name
        result += "," + str(self.datastream_id)
        result += "," + self.request_type.name
        result += "," + str(self.params)
        if (includeUsersName):
            result += "," + self.users_feed_name
        if(includesFeedInfo):
            result += "," + json.dumps(self.feed_info)

        return result

    def user_defined_name(self):
        return self.csv_line_string(True)[7]