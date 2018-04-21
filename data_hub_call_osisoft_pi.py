from data_hub_call import Data_hub_call
import requests
import requests.packages.urllib3.exceptions
import json

from requests.packages.urllib3.exceptions import InsecureRequestWarning


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Data_hub_call_osisoft_pi(Data_hub_call):

    core_URL = "https://130.88.97.137/piwebapi"

    host = core_URL.replace('https://', '').replace('http://', '').replace('/piwebapi', '')

    def __init__(self, request_info):
        self.request_info = request_info


    def call_api_fetch(self, get_latest_only=True):
        """
        GET https: // myserver / piwebapi / assetdatabases / D0NxzXSxtlKkGzAhZfHOB - KAQLhZ5wrU - UyRDQnzB_zGVAUEhMQUZTMDRcTlVHUkVFTg HTTP / 1.1
        Host: myserver
        Accept: application / json"""

        output_format = 'application/json'
        url_string = self.request_info.url_string()


        # passing the username and required output format
        headers_list = {"Accept": output_format, "Host": self.request_info.host}

        try:
            hub_result = requests.get(url_string, headers=headers_list, timeout=10.000, verify=False)
            if hub_result.ok == False:
                raise ConnectionRefusedError("Connection to Triangulum hub refused: " + hub_result.reason)
        except:
            raise ConnectionError("Error connecting to Triangulum hub - check internet connection.")

        result = {}
        result_content_json = hub_result.json()
        result['ok'] = hub_result.ok

        result['content'] = json.dumps(result_content_json)
        if "Items" in result_content_json:
            available_matches = len(result_content_json['Items'])
        else:
            available_matches = 1

        # No Date params allowed in call to hub, so apply get latest only to hub results here...
        if (get_latest_only and self.request_info.last_fetch_time != None):
            try:
                # Filter python objects with list comprehensions
                new_content = [x for x in result_content_json['Items']
                          if self.get_date_time(x['Timestamp']) > self.request_info.last_fetch_time]

                result_content_json['Items'] = new_content
                result['content'] = json.dumps(result_content_json)
                result['ok'] = True
            except ValueError as e:
                result['ok'] = False
                result['reason'] = str(e)
            except Exception as e:
                result['ok'] = False
                result['reason'] = 'Problem sorting results by date to get latest only. ' + str(e)

        result['available_matches'] = available_matches
        if 'Items' in result_content_json:
            result['returned_matches'] = len(result_content_json['Items'])
        else:
            result['returned_matches'] = 1


        # Set last_fetch_time for next call
        if (get_latest_only):
            if (len(result_content_json['Items']) > 0):
                try:
                    newlist = sorted(result_content_json['Items'],
                                     key=lambda k: self.get_date_time(k["Timestamp"]),
                                     reverse=True)

                    most_recent = newlist[0]["Timestamp"]
                    self.request_info.last_fetch_time = self.get_date_time(most_recent)
                except ValueError as e:
                    result['ok'] = False
                    result['reason'] = str(e)
                except Exception as e:
                    result['ok'] = False
                    result['reason'] = 'Problem sorting results by date to get latest only. ' + str(e)

        return result

