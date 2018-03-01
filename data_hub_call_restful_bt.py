from data_hub_call import Data_hub_call
import requests
import json

class Data_hub_call_restful_bt(Data_hub_call):

    core_URL = "http://api.bt-hypercat.com"

    def __init__(self, request_info): #, username, api_key):
        """Return a BT data hub connection object which will
            be used to connect to [stream] using [credentials]
         """
        self.request_info = request_info


    def call_api_fetch(self, params, output_format='application/json', get_latest_only=True):
        result = {}

        url_string = self.request_info.url_string()


        # passing the username and required output format
        headers_list = {"x-api-key": self.request_info.username, "Accept": output_format}
        #return "Username: " +cred.username + "; Api-key: " + cred.api_key + "; url: " + url_string

        if(get_latest_only and self.request_info.last_fetch_time != None):
            #Start date needs to be in format: 2015-05-07T12:52:00Z
            params['start'] = self.request_info.last_fetch_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            if 'start' in params:
                del params['start']

        hub_result = requests.get(url_string,
                            timeout=10.000,
                            auth=(self.request_info.api_key, ':'),
                            params=params,
                            headers=headers_list)


        result['ok'] = hub_result.ok
        if (hub_result.ok):
            result_content = hub_result.content.decode("utf-8")
        else:
            result_content = '{}'
            result['reason'] = hub_result.reason

        json_result_content = json.loads(result_content)
        newlist = []

        if(hub_result.ok and get_latest_only):
            if(len(json_result_content) > 0):
                try:
                    newlist = sorted(json_result_content,
                                     key=lambda k: self.get_date_time(k["time"]),
                                     reverse=True)
                    most_recent = newlist[0]["time"]
                    self.request_info.last_fetch_time = self.get_date_time(most_recent)
                except ValueError as e:
                    result['ok'] = False
                    result['reason'] = str(e)
                except Exception as e:
                    result['ok'] = False
                    result['reason'] = 'Problem sorting results by date to get latest only. ' + str(e)
        result['content'] = json.dumps(json_result_content)
        result['available_matches'] = len(json_result_content)
        result['returned_matches'] = len(newlist)
        return result

    def call_api_post(self, eeml):
        # registered user name
        #username = "AnnGledson"
        # API key given to the user
        #api_key = '8a0b0d8c-bf4e-44d5-b34d-a2d5f139918a'  # AnnGledson2
        # choose output format - can be json or xml
        output_format = 'application/json'
        url_string = self.request_info.url_string()

        # 'http://api.bt-hypercat.com/'+ stream.feedType+'/feeds/'+stream.feed_id+'/datastreams/'+\
        #             stream.datastream_id+'/datapoints'

        # -----------------------------------#
        # Request URL Parameters

        # start_dt = datetime.strptime( "2017-01-01 00:00:00", "%Y-%m-%d %H:%M:%S" )
        # payload = {'start': start_dt}

        #for cred in self.credentials:
        # passing the username and required output format
        headers_list = {"x-api-key": self.request_info.username, "Accept":"*/*", "Content-Type":"text/xml"}
        #-H 'Content-Type:text/xml' -H 'Accept:*/*'
        #-X POST -d@{filename}

        return requests.post(url_string,
                               eeml,
                                auth=(self.request_info.api_key, ':'),
                                headers=headers_list)
