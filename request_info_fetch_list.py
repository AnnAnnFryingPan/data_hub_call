from request_info_list import Request_info_list
from request_info import Request_info
from request_info_restful_bt_fetch import Request_info_restful_bt_fetch, Feed_type, Request_type, Request_type_ds_or_features
from request_info_restful_cdp_fetch import Request_info_restful_cdp_fetch, Feed_type, Request_type, Request_type_ds_or_features
from request_info_osisoft_pi_fetch import Request_info_osisoft_pi_fetch, Feed_type_pi, Request_type_pi
from enum import Enum
from ast import literal_eval
import json

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

    def append_restful_bt_request_list(self, username, api_key, request_params_csv_list):

        # import hypercat streams
        for line in request_params_csv_list:
            try:
                json_line = json.loads(line)
                self.append_restful_bt_request_json(username, api_key, json_line)
            except:
                self.append_restful_bt_request(username, api_key, line)

    def append_restful_bt_request_json(self, username, api_key, json_line):
        core_url_string = json_line['stream_params'][0]
        host = core_url_string.replace('https://', '').replace('http://', '').replace('/piwebapi', '')
        feed_type = Feed_type[json_line['stream_params'][1]]
        stream_id = json_line['stream_params'][2]

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
            self.requests.append(Request_info_restful_bt_fetch(api_key, username,
                core_url_string, feed_type, stream_id, request_type_ds_or_features, datastream_id,
                request_type, params_list_str, users_feed_name, feed_info))
            return self.requests[len(self.requests)-1]

        except:
            #raise;
            print("Error adding new hypercat api stream to list: " + json.dumps(json_line))


    def append_restful_bt_request(self, username, api_key, request_params_csv_line):
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
            self.requests.append(Request_info_restful_bt_fetch(api_key, username,
                core_url_string, feed_type, feed_id, request_type_ds_or_features, datastream_id,
                request_type, params_list_str, users_feed_name, feed_info))
            return self.requests[len(self.requests)-1]

        except:
            #raise;
            print("Error adding new hypercat api stream to list: " + request_params_csv_line)

    def append_restful_cdp_request_list(self, username, api_key, request_params_csv_list):

        # import hypercat streams
        for line in request_params_csv_list:
            try:
                json_line = json.loads(line)
                test = json_line['stream_params']
                self.append_restful_cdp_request_json(username, api_key, json_line)
            except:
                self.append_restful_cdp_request(username, api_key, line)

    def append_restful_cdp_request(self, username, api_key, request_params_csv_line):
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

        if (len(list_params) > 8):
            str_feed_info = ','.join(list_params[8:])
            feed_info = json.loads(str_feed_info)
        else:
            feed_info = {}

        try:
            self.requests.append(Request_info_restful_cdp_fetch(api_key, username,
                                                               core_url_string, feed_type, feed_id,
                                                               request_type_ds_or_features, datastream_id,
                                                               request_type, params_list_str, users_feed_name,
                                                               feed_info))
            return self.requests[len(self.requests) - 1]

        except:
            # raise;
            print("Error adding new hypercat api stream to list: " + request_params_csv_line)

    def append_restful_cdp_request_json(self, username, api_key, json_line):
        core_url_string = json_line['stream_params'][0]
        host = core_url_string.replace('https://', '').replace('http://', '').replace('/piwebapi', '')
        feed_type = Feed_type[json_line['stream_params'][1]]
        stream_id = json_line['stream_params'][2]

        request_type_ds_or_features = Request_type_ds_or_features[json_line['stream_params'][3]]
        datastream_id = int(json_line['stream_params'][4])
        request_type = Request_type[json_line['stream_params'][5]]
        params_list_str = literal_eval(json_line['stream_params'][6].rstrip('\n'))  # {'limit': '100'} '{\\'limit\\':\\'100\\'}'


        try:
            users_feed_name = json_line['stream_params'][7].rstrip('\n')
        except:
            users_feed_name = ''

        if 'feed_info' in json_line:
            feed_info = json_line['feed_info']
        else:
            feed_info = {}

        try:
            self.requests.append(Request_info_restful_cdp_fetch(api_key, username,
                core_url_string, feed_type, stream_id, request_type_ds_or_features, datastream_id,
                request_type, params_list_str, users_feed_name, feed_info))
            return self.requests[len(self.requests)-1]

        except:
            #raise;
            print("Error adding new hypercat api stream to list: " + json.dumps(json_line))

    def append_pi_request_list(self, request_params_csv_list, metadata=False):
        # import PI streams

        for line in request_params_csv_list:
            try:
                json_line = json.loads(line)
                test = json_line['stream_params']
                self.append_pi_request_json(json_line)
            except:
                self.append_pi_request(line, metadata)

    def append_pi_request_json(self, json_line, metadata=False):
        # https://130.88.97.137/piwebapi,streams,A0EXpbRmwnc7kq0OSy1LydJJQxey0y1BT5hGA3gBQVqtCQgLR6A7xNbjk6RXQ6dns2
        # VqAVk0tUEktUDAxLkRTLk1BTi5BQy5VS1xUUklBTkdVTFVNXEFJUiBRVUFMSVRZXE9YRk9SRCBST0FEfE5PIExFVkVM,
        # interpolated,Anns_Pi_feed_Nitrogen
        core_url_string = json_line['stream_params'][0]
        host = core_url_string.replace('https://', '').replace('http://', '').replace('/piwebapi', '')
        stream_id = json_line['stream_params'][2]
        if (metadata):
            feed_type = Feed_type_pi.attributes
            request_type = Request_type_pi.none
        else:
            feed_type = Feed_type_pi[json_line['stream_params'][1]]
            request_type = Request_type_pi[json_line['stream_params'][3].rstrip('\n')]
        try:
            users_feed_name = json_line['user_defined_name'].rstrip('\n')
        except:
            users_feed_name = ''

        if 'feed_info' in json_line:
            feed_info = json_line['feed_info']
        else:
            feed_info = {}

        try:
            return self.append_pi_request_core(host, core_url_string, feed_type, stream_id, request_type,
                                              users_feed_name, feed_info)
        except:
            print("Error adding new Osisoft Pi api stream to list: " + json.dumps(json_line))

    def append_pi_request(self, request_params_csv_line, metadata=False):
        list_params = request_params_csv_line.split(",")
        # https://130.88.97.137/piwebapi,streams,A0EXpbRmwnc7kq0OSy1LydJJQxey0y1BT5hGA3gBQVqtCQgLR6A7xNbjk6RXQ6dns2
        # VqAVk0tUEktUDAxLkRTLk1BTi5BQy5VS1xUUklBTkdVTFVNXEFJUiBRVUFMSVRZXE9YRk9SRCBST0FEfE5PIExFVkVM,
        # interpolated,Anns_Pi_feed_Nitrogen
        core_url_string = list_params[0]
        host = core_url_string.replace('https://', '').replace('http://', '').replace('/piwebapi', '')
        param_list = list_params[2]
        if (metadata):
            feed_type = Feed_type_pi.attributes
            request_type = Request_type_pi.none
        else:
            feed_type = Feed_type_pi[list_params[1]]
            request_type = Request_type_pi[list_params[3].rstrip('\n')]
        try:
            users_feed_name = list_params[4].rstrip('\n')
        except:
            users_feed_name = ''

        if (len(list_params) > 5):
            str_feed_info = ','.join(list_params[5:])
            feed_info = json.loads(str_feed_info)
        else:
            feed_info = {}

        try:
            return self.append_pi_request_core(host, core_url_string, feed_type, param_list, request_type,
                                              users_feed_name, feed_info)
        except:
            print("Error adding new Osisoft Pi api stream to list: " + request_params_csv_line)



    def append_pi_request_core(self, host, core_url_string, feed_type, param_list, request_type, users_feed_name,
                               feed_info):
            # (api_core_url, feed_type, feed_id, request_type_1, datastream_id,
            #   request_type_2, params, users_feed_name):
            self.requests.append(Request_info_osisoft_pi_fetch(
                host, core_url_string, feed_type, param_list, request_type, users_feed_name, feed_info))
            return self.requests[len(self.requests) - 1]


    def append(self, stream):
        self.requests.append(stream)

    def get_list_of_users_stream_ids(self):
        result = []

        for request in self.requests:
            result.append(request.users_feed_name)

        return result









