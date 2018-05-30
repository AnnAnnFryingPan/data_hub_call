import os
from data_hub_call_restful_bt import Data_hub_call_restful_bt
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi
from data_hub_call_restful_cdp import Data_hub_call_restful_cdp
from request_info_fetch_list import Request_info_fetch_list, Data_request_type
import json


class Selected_streams(object):
    CORE_API_BT = Data_hub_call_restful_bt.CORE_URL
    CORE_API_CDP = Data_hub_call_restful_cdp.CORE_URL
    CORE_API_TRIANGULUM = Data_hub_call_osisoft_pi.CORE_URL

    def __init__(self, data_sources_path):
        self.data_source_dir = data_sources_path
        self.restful_bt_sources_dir = 'restful_bt_sources'
        self.bt_requests_filename = 'list_restful_bt_requests.json'
        self.bt_credentials_filename = 'bt_hub_credentials.csv'
        self.restful_triangulum_sources_dir = 'osisoft_pi_sources'
        self.triangulum_requests_filename = 'list_osisoft-pi_requests.json'
        self.cdp_sources_dir = 'cdp_sources'
        self.cdp_requests_filename = 'list_cdp_requests.json'
        self.cdp_credentials_filename = 'cdp_credentials.csv'


        # Check folders are present and create if not.
        file_name_bt = os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_requests_filename)
        file_name_tri = os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir,
                                     self.triangulum_requests_filename)
        file_name_cdp = os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_requests_filename)
        if not os.path.exists(os.path.dirname(file_name_bt)):
            os.makedirs(os.path.dirname(file_name_bt), exist_ok=True)
        if not os.path.exists(os.path.dirname(file_name_tri)):
            os.makedirs(os.path.dirname(file_name_tri), exist_ok=True)
        if not os.path.exists(os.path.dirname(file_name_cdp)):
            os.makedirs(os.path.dirname(file_name_cdp), exist_ok=True)

        try:
            self.api_streams = Request_info_fetch_list()
        except Exception as err:
            raise err

    def write_credentials(self, hub, credential):
        # 8a0b0d8c-bf4e-44d5-b34d-a2d5f139918a,AnnGledson

        if hub.api_core_url == Data_hub_call_restful_bt.CORE_URL:
            # 1. BT Hub
            file_name = os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_credentials_filename)
            cred_csv_line = credential['api_key'] + ',' + credential['username']

            if not os.path.exists(os.path.dirname(file_name)):
                os.makedirs(os.path.dirname(file_name), exist_ok=True)

            try:
                with open(file_name, "w") as fp:
                    fp.write(cred_csv_line)
                    fp.close()
            except Exception as err:
                raise IOError('Unable to open or write to credentials file: ' + file_name + '. ' + str(err))

        #elif hub.api_core_url == Data_hub_call_osisoft_pi.CORE_URL:
            # 2. Triangulum
            # Triangulum doesn't require credentials to access streams
            #file_name = os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_credentials_filename)
        elif hub.api_core_url == Data_hub_call_restful_cdp.CORE_URL:
            # 3. CDP
            file_name = os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_credentials_filename)
            cred_csv_line = credential['api_key'] + ',' + credential['username']

            if not os.path.exists(os.path.dirname(file_name)):
                os.makedirs(os.path.dirname(file_name), exist_ok=True)

            try:
                with open(file_name, "w") as fp:
                    fp.write(cred_csv_line)
                    fp.close()
            except Exception as err:
                raise IOError('Unable to open or write to credentials file: ' + file_name + '. ' + str(err))

    def get_selected_stream_ids(self):
        return self.api_streams.get_list_of_users_stream_ids()


    def get_streams_from_file(self):
        # Read from selected streams files
        self.api_streams.clear_all()
        api_streams_json = None


        # Get the BT credentials from the BT folder
        # EG: 8a0b0d8c-bf4e-44d5-b34d-a2d5f139918a,AnnGledson
        try:
            with open(os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_credentials_filename) \
                    , "r+") as f_credentials:
                bt_credentials = f_credentials.readline().split(',')
                api_key = bt_credentials[0]
                username = bt_credentials[1]
        except Exception as err:
            print(
                'Unable to read BT credentials file ' + self.bt_credentials_filename + ' file in '
                + self.restful_bt_sources_dir + '. ' + str(err))
        else:
            # Get the streams as JSON from the BT file
            try:
                with open(os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_requests_filename)) \
                        as f_requests:
                    api_streams_json = json.load(f_requests)
            except Exception as err:
                print(
                    'Unable to read BT streams file ' + self.bt_requests_filename + ' file in '
                    + self.restful_bt_sources_dir + '. ' + str(err))

        for stream_params in api_streams_json:
            try:
                self.api_streams.append_request(stream_params, api_key, username)
            except Exception as err:
                print('Unable to poll BT stream: ' + json.dumps(stream_params) + '... ' + str(err))
        api_streams_json = {}

        # Get the streams as CSV from the Triangulum file
        try:
            with open(os.path.join(
                    self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_requests_filename)) \
                    as f_requests:
                api_streams_json = json.load(f_requests)
        except Exception as err:
            print('Unable to read Triangulum streams file ' + self.triangulum_requests_filename + ' file in '
                  + self.restful_triangulum_sources_dir + '. ' + str(err))

        for stream_params in api_streams_json:
            try:
                self.api_streams.append_request(stream_params)
            except Exception as err:
                print('Unable to poll Triangulum stream: ' + json.dumps(stream_params) + '... ' + str(err))

        api_streams_json = {}

        try:
            with open(os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_credentials_filename), \
                      "r+") as f_creds:
                cdp_credentials = f_creds.readline().split(',')
                list_params = cdp_credentials[0]
                api_key = list_params.rstrip('\n')
        except Exception as err:
            print('Unable to read CDP credentials file ' + self.cdp_credentials_filename + ' file in '
            + self.cdp_sources_dir + '. ' + str(err))
        else:
            try:
                with open(os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_requests_filename)) \
                        as f_requests:
                    api_streams_json = json.load(f_requests)
            except Exception as err:
                print('Unable to read CDP streams file ' + self.cdp_requests_filename + ' file in '
                    + self.cdp_sources_dir + '. ' + str(err))


        for stream_params in api_streams_json:
            try:
                self.api_streams.append_request(stream_params, api_key)
            except Exception as err:
                print('Unable to poll CDP stream: ' + stream_params_str + '... ' + str(err))


    def clear_all_streams(self):
        self.api_streams.clear_all()
        new_file = []

        if os.path.exists(os.path.dirname(os.path.join(self.data_source_dir, self.restful_bt_sources_dir))):
            with open(os.path.join(self.data_source_dir, self.restful_bt_sources_dir,
                                   self.bt_requests_filename), "w+") as f_out:
                f_out.writelines(new_file)
        if os.path.exists(os.path.dirname(os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir))):
            with open(os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir,
                                        self.triangulum_requests_filename), "w+") as f_out:
                f_out.writelines(new_file)
        if os.path.exists(os.path.dirname(os.path.join(self.data_source_dir, self.cdp_sources_dir))):
            with open(os.path.join(self.data_source_dir, self.cdp_sources_dir,
                                        self.cdp_requests_filename), "w+") as f_out:
                f_out.writelines(new_file)


    def remove_from_streams(self, stream_params):
        hub_url = stream_params['stream_params'][0]
        stream_href = stream_params['feed_info']['href']

        if (hub_url == Data_hub_call_restful_bt.CORE_URL):  # "http://api.bt-hypercat.com"):
            file_name = os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_requests_filename)
        elif (hub_url == Data_hub_call_osisoft_pi.CORE_URL):  # "https://130.88.97.137/piwebapi"):
            file_name = os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_requests_filename)
        elif (hub_url == Data_hub_call_restful_cdp.CORE_URL):
            file_name = os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_requests_filename)
        else:
            raise NameError('Unrecognised hub url: ' + hub_url)

        if not os.path.exists(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name), exist_ok=True)

        try:
            with open(file_name, "w+")  as f_requests:
                try:
                    api_streams_json = json.load(f_requests)['stream_requests']
                except:
                    api_streams_json = []
                for api_stream in api_streams_json:
                    if api_stream['feed_info']['href'] == stream_href:
                        del api_streams
                json.dump(api_streams_json, f_requests)
        except Exception as err:
            raise IOError('Unable to open streams file: ' + file_name + '. ' + str(err))

        self.get_streams_from_file()



    def add_to_streams(self, new_stream_params):
        str_stream_params = json.dumps(new_stream_params)
        hub_url = new_stream_params['stream_params'][0]
        stream_href = new_stream_params['feed_info']['href']

        if (hub_url == Data_hub_call_restful_bt.CORE_URL):  # "http://api.bt-hypercat.com"):
            file_name = os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_requests_filename)
        elif (hub_url == Data_hub_call_osisoft_pi.CORE_URL):  # "https://130.88.97.137/piwebapi"):
            file_name = os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_requests_filename)
        elif (hub_url == Data_hub_call_restful_cdp.CORE_URL):
            file_name = os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_requests_filename)
        else:
            raise NameError('Unrecognised hub url: ' + hub_url)

        if not os.path.exists(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name), exist_ok=True)

        try:
            with open(file_name, "w+")  as f_requests:
                try:
                    api_streams_json = json.load(f_requests)['stream_requests']
                except:
                    api_streams_json = []
                for api_stream in api_streams_json:
                    if api_stream['feed_info']['href'] == stream_href:
                        break
                else:
                    api_streams_json.append(new_stream_params)
                json.dump(api_streams_json, f_requests)

        except Exception as err:
            raise IOError('Unable to open streams file: ' + file_name + '. ' + str(err))

        self.get_streams_from_file()

    def get_previously_selected_streams(self, user, streams):
        previous_streams_list = {'streams': []}

        for stream in streams:
            stream_dict = {}
            stream_dict["datetime"] = str(stream.added_date)
            str_params = stream.parameters.replace("'", '"')
            stream_dict["parameters"] = json.loads(str_params)
            stream_dict["users_name"] = user.username
            previous_streams_list['streams'].append(stream_dict)

        return previous_streams_list