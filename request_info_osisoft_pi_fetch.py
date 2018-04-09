from request_info_osisoft_pi import Request_info_osisoft_pi, Feed_type_pi, Request_type_pi
import json

class Request_info_osisoft_pi_fetch(Request_info_osisoft_pi):
    """A data stream from any hypercat platform/hub:
    """

    def __init__(self, request_params, metadata=False):
        try:
            json_line = json.loads(request_params)
            self.init_pi_request_json(json_line)
        except:
            self.init_pi_request(request_params, metadata)



    def init_pi_request_json(self, request_params_json, metadata=False):
        # https://130.88.97.137/piwebapi,streams,A0EXpbRmwnc7kq0OSy1LydJJQxey0y1BT5hGA3gBQVqtCQgLR6A7xNbjk6RXQ6dns2
        # VqAVk0tUEktUDAxLkRTLk1BTi5BQy5VS1xUUklBTkdVTFVNXEFJUiBRVUFMSVRZXE9YRk9SRCBST0FEfE5PIExFVkVM,
        # interpolated,Anns_Pi_feed_Nitrogen
        core_url_string = request_params_json['stream_params'][0]
        host = core_url_string.replace('https://', '').replace('http://', '').replace('/piwebapi', '')
        params = request_params_json['stream_params'][2]
        if (metadata):
            feed_type = Feed_type_pi.attributes
            request_type = Request_type_pi.none
        else:
            feed_type = Feed_type_pi[request_params_json['stream_params'][1]]
            request_type = Request_type_pi[request_params_json['stream_params'][3].rstrip('\n')]
        try:
            users_feed_name = request_params_json['user_defined_name'].rstrip('\n')
        except:
            users_feed_name = ''

        if 'feed_info' in request_params_json:
            feed_info = request_params_json['feed_info']
        else:
            feed_info = {}

        try:
            super(Request_info_osisoft_pi_fetch, self).__init__(host,
                                                                core_url_string,
                                                                feed_type,
                                                                params,
                                                                request_type,
                                                                users_feed_name,
                                                                feed_info)
        except:
            print("Error creating new request (triangulum): " + json.dumps(request_params_json))

    def init_pi_request(self, request_params_csv_line, metadata=False):
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
            super(Request_info_osisoft_pi_fetch, self).__init__(host,
                                                                core_url_string,
                                                                feed_type,
                                                                param_list,
                                                                request_type,
                                                                users_feed_name,
                                                                feed_info)
        except:
            print("Error creating new request (triangulum): " + request_params_csv_line)



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

