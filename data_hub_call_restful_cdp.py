from data_hub_call import Data_hub_call
import requests
import json

#https://api.cityverve.org.uk/v1/entity/noise-meter/6472W/timeseries/dose/datapoints


class Data_hub_call_restful_cdp(Data_hub_call):

    #core_URL_test = "http://35.176.23.252:8000"
    core_URL = "https://api.cityverve.org.uk"

    def __init__(self, request_info): #, username, api_key):
        """Return a CDP connection object which will
            be used to connect to [stream] using [credentials]
         """
        self.request_info = request_info


    def call_api_fetch(self, params, output_format='application/json', get_latest_only=True,
                       get_children_as_time_series=True, time_field="entity.occurred", value_field="value"):
        result = {}

        # Make request to CDP hub
        url_string = self.request_info.url_string()
        temp_get_latest = False if (get_children_as_time_series == True) else get_latest_only;
        hub_result = self.get_request(url_string, params, output_format, temp_get_latest)


        result_content = hub_result.content.decode("utf-8")


        json_result_content = json.loads(result_content)

        ###### Due to CDP having to fetch children [1] ######
        # Get entity children
        if get_children_as_time_series:
            json_result_children = []
            for entity in json_result_content:
                if 'uri' in entity:
                    child_uri = entity['uri']
                elif 'url' in entity:
                    child_uri = entity['url']
                elif 'href' in entity:
                    child_uri = entity['href']
                child_result = self.get_child_for_time_series(child_uri, params, output_format, get_latest_only)
                if child_result is not None:
                    json_result_children.append(child_result)
            json_result_content = self.get_children_as_time_series(json_result_children, time_field=time_field)

        available_matches = len(json_result_content)

        # No Date params allowed in call to hub, so apply get latest only to hub results here...
        if (get_latest_only and self.request_info.last_fetch_time != None):
            try:
                # Filter python objects with list comprehensions
                new_content = [x for x in json_result_content
                               ###### Due to CDP having to fetch children [2] ######
                               # For CDP we also include results with time == last_fetch_time (>=), as we have to keep
                               # aggregating all results with same time, so latest time value might still be incrementing
                               if self.get_date_time(x['time']) >= self.request_info.last_fetch_time]

                json_result_content = new_content
                result['content'] = json.dumps(json_result_content)
            except ValueError as e:
                result['ok'] = False
                result['reason'] = str(e)
            except Exception as e:
                result['ok'] = False
                result['reason'] = 'Problem sorting results by date to get latest only. ' + str(e)

        result['available_matches'] = available_matches
        result['returned_matches'] = len(json_result_content)

        # Set last_fetch_time for next call
        newlist = []
        if get_latest_only:
            if len(json_result_content) > 0:
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

        # Return result
        result['content'] = json.dumps(json_result_content)
        return result

    def get_request(self, url, params, output_format, get_latest_only):
        # passing the username and required output format
        headers_list = {    "Authorization": self.request_info.api_key,
                            "Accept": output_format}

        if (get_latest_only and self.request_info.last_fetch_time != None):
            # Start date needs to be in format: 2015-05-07T12:52:00Z
            params['start'] = self.request_info.last_fetch_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            if 'start' in params:
                del params['start']

        try:
            hub_result = requests.get(url,
                                timeout=10.000,
                                params=params,
                                headers=headers_list)
            if hub_result.ok == False:
                raise ConnectionRefusedError("Connection to CDP refused: " + hub_result.reason)
        except:
            raise ConnectionError("Error connecting to CDP hub - check internet connection.")

        return hub_result




    def get_child_for_time_series(self, uri, params, output_format='application/json', get_latest_only=True):
        # Make request to CDP hub
        hub_result = self.get_request(uri, params, output_format, get_latest_only)
        return json.loads(hub_result.content.decode("utf-8"))


    def get_children_as_time_series(self, json_children, time_field="entity.occurred", value_field="value"):
        result = []
        time_field_as_path_list = time_field.strip().split(".")
        if len(time_field_as_path_list) == 0:
            return result

        """
        FROM:
        {
            "id": "60411677",
            "uri": "https://api.cityverve.org.uk/v1/entity/crime/60411677",
            "type": "crime",
            "name": "Anti Social Behaviour - On or near Manor Road",
            "loc": {},
            "entity": {
                "category": "Anti Social Behaviour",
                "occurred": "2017-10-01",
                "area": "On or near Manor Road",
                "outcome": {
                    "status": null,
                    "resolved": null
                }
            },
            "instance": {...},
            "legal": [..]
        }, {}, {}, ...

        TO:
        {
            "time": "2017-10-01",
            "value": "556"
        },
        {...}, {...}, ..."""


        # get times
        result_dict = {}
        for child in json_children:
            time = self.get_time(child[0], time_field_as_path_list)
            if time in result_dict:
                result_dict[time] += 1
            else:
                result_dict[time] = 1

        # turn lookup dict (result_dict) into final list
        for key, value in result_dict.items():
            temp = {'time': key, 'value': value}
            result.append(temp)

        return result

    def get_time(self, json_dict, list_path):
        #2018-02-23T08:24:38.127Z or 2017-10-01
        time = self.get_val_from_path(json_dict, list_path)
        date_time = self.get_date_time(time)
        if date_time.time():
            # Contains time, so group_by = hour
            time = str(date_time.date()) + 'T' + str(date_time.hour) + ':00:00.000Z'
        #Otherwise leave as it and group_by will be daily (or monthly or yearly depending on granularity)
        return time


    def get_val_from_path(self, json_dict, list_path):
        result = json_dict
        for val_field in list_path:
            result = result[val_field]
        return result

