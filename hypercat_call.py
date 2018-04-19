import requests
import json

class Hypercat_call(object):
    """A connection/call to the BT Hypercat search facility"""

    def call_hypercat_search(self, url, cat_url, json_content={}, request_type='get', headers_list={}):
        result = {}

        if cat_url:
            payload = {'url': cat_url}
        else:
            payload = {}

        try:
            if(str(request_type).strip().lower() == 'get'):
                search_result = requests.get(url,
                                             timeout=20.000,
                                             #auth=auth,
                                             headers=headers_list)
            elif(str(request_type).strip().lower() =='post'):
                search_result = requests.post(url,
                                    headers=headers_list,
                                    params=payload,
                                    timeout=20.000,
                                    json= json_content)
        except Exception as err:
            result['ok'] = False
            result['content'] = err
            return result

        try:
            result_content = search_result.json()
        except:
            try:
                result_content = json.loads(search_result.content)
            except:
                try:
                    result_content = search_result.content
                except Exception as err2:
                    result['ok'] = False
                    result['content'] = err2
                    return result

        result['ok'] = search_result.ok
        result['content'] = result_content

        return result

    def call_hypercat_search_cdp(self, url, headers_list={}):
        result = {}

        try:
            search_result = requests.get(url,
                                         timeout=20.000,
                                         headers=headers_list)
        except Exception as err:
            result['ok'] = False
            result['content'] = err
            return result

        try:
            result_content = search_result.json()
        except:
            try:
                result_content = json.loads(search_result.content)
            except:
                try:
                    result_content = search_result.content
                except Exception as err2:
                    result['ok'] = False
                    result['content'] = err2
                    return result

        result['ok'] = search_result.ok
        result['content'] = result_content

        return result

    def get_hypercat_catalogue(self, url, headers_list={}):
        result = {}

        try:
            search_result = requests.get(url,
                                         timeout=50.000,
                                         headers=headers_list,
                                         )
        except Exception as err:
            result['ok'] = False
            result['content'] = str(err)
            return result

        if search_result.ok == True:
            try:
                result['content'] = json.loads(search_result.content.decode("utf-8"))
            except:
                try:
                    result['content'] = json.loads(json.dumps(search_result.json()))
                except Exception as err:
                    result['ok'] = False
                    result['content'] = str(err)
                    return result
        else:
            result['content'] = search_result.reason

        result['ok'] = search_result.ok
        return result















