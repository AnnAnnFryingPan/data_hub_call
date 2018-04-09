from request_info_restful_bt import Request_info_restful_bt, Feed_type, Request_type
from enum import Enum
import json
from ast import literal_eval

class Request_type_ds_or_features(Enum):
    none = 0
    datastreams = 1
    features = 2


class Request_info_restful_bt_fetch(Request_info_restful_bt):
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

    def __init__(self, username, api_key, request_params):
        try:
            json_request_params = json.loads(request_params)
            self.init_json(username, api_key, json_request_params)
        except:
            self.init_string(username, api_key, request_params)



    def init_json(self, username, api_key, json_line):
        core_url = json_line['stream_params'][0]
        feed_type = Feed_type[json_line['stream_params'][1]]
        feed_id = json_line['stream_params'][2]

        request_type_ds_or_features = Request_type_ds_or_features[json_line['stream_params'][3]]
        datastream_id = int(json_line['stream_params'][4])
        request_type = Request_type[json_line['stream_params'][5]]
        params_list_str = literal_eval(json_line['stream_params'][6].rstrip('\n'))  # {'limit': '100'} '{\\'limit\\':\\'100\\'}'


        try:
            users_feed_name = json_line['user_defined_name'].rstrip('\n')
        except:
            users_feed_name = ''

        if 'feed_info' in json_line:
            feed_info = json_line['feed_info']
        else:
            feed_info = {}

        try:
            super(Request_info_restful_bt_fetch, self).__init__(api_key,
                                                                username,
                                                                core_url,
                                                                feed_type,
                                                                feed_id,
                                                                datastream_id,
                                                                request_type,
                                                                users_feed_name,
                                                                feed_info)
            self.request_type_ds_or_feature = request_type_ds_or_features
            self.params = params_list_str
        except:
            #raise;
            print("Error creating new request (bt): " + json.dumps(json_line))




    def init_string(self, username, api_key, request_params_csv_line):
        # import hypercat stream
        list_params = request_params_csv_line.split(",")
        # http://api.bt-hypercat.com sensors 86a25d4e-25fc-4ebf-a00d-0a603858c7e1 datastreams 0 datapoints {} anns_feed_1
        core_url_string = list_params[0]
        feed_type = Feed_type[list_params[1]]
        feed_id = list_params[2]

        request_type_ds_or_features = Request_type_ds_or_features[list_params[3]]
        datastream_id = int(list_params[4])
        request_type = Request_type[list_params[5]]
        params_list_str = literal_eval(list_params[6].rstrip('\n'))  # {'limit': '100'} '{\\'limit\\':\\'100\\'}'


        try:
            users_feed_name = list_params[7].rstrip('\n')
        except:
            users_feed_name = ''

        if(len(list_params) > 8):
            str_feed_info = ','.join(list_params[8:])
            feed_info = json.loads(str_feed_info)
        else:
            feed_info = {}

        try:
            super(Request_info_restful_bt_fetch, self).__init__(api_key, username,
                                                                core_url_string,
                                                                feed_type,
                                                                feed_id,
                                                                datastream_id,
                                                                request_type,
                                                                users_feed_name,
                                                                feed_info)
            self.request_type_ds_or_feature = request_type_ds_or_features
            self.params = params_list_str
        except:
            #raise;
            print("Error creating new request (bt): " + request_params_csv_line)


    def url_string(self):
        result = self.api_core_url + '/' + Feed_type(self.feed_type).name + '/feeds/' + self.feed_id

        if self.request_type_ds_or_feature.value > 0:
            result += "/" + self.request_type_ds_or_feature.name
        if self.datastream_id > -1:
            result += "/" + str(self.datastream_id)
        if self.request_type.value > 0:
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
        return self.users_feed_name