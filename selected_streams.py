import os
from data_hub_call_restful_bt import  Data_hub_call_restful_bt
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi
from data_hub_call_restful_cdp import Data_hub_call_restful_cdp
from request_info_fetch_list import Request_info_fetch_list, Data_request_type
import json


class Selected_streams(object):
    def __init__(self, data_sources_path, identifier):
        self.identifier = identifier
        self.data_source_dir = data_sources_path
        self.restful_bt_sources_dir = 'restful_bt_sources'
        self.bt_requests_filename = 'list_restful_bt_requests.csv'
        self.bt_credentials_filename = 'bt_hub_credentials.csv'
        self.restful_triangulum_sources_dir = 'osisoft_pi_sources'
        self.triangulum_requests_filename = 'list_osisoft-pi_requests.csv'
        self.cdp_sources_dir = 'cdp_sources'
        self.cdp_requests_filename = 'list_cdp_requests.csv'
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

        self.api_streams = Request_info_fetch_list()

    def write_credentials(self, hub, credential):
        # 8a0b0d8c-bf4e-44d5-b34d-a2d5f139918a,AnnGledson

        if (hub.api_core_url == "http://api.bt-hypercat.com"):
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
                print(
                    'Unable to open or write to credentials file: ' + file_name + '. ' + str(err))

        #elif (hub.api_core_url == "https://130.88.97.137/piwebapi"):
            # 2. Triangulum
            # Triangulum doesn't require credentials to access streams
            #file_name = os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_credentials_filename)
        elif (hub.api_core_url == "https://api.cityverve.org.uk/v1/"):
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
                print(
                    'Unable to open or write to credentials file: ' + file_name + '. ' + str(err))

    def get_selected_stream_ids(self):
        return self.api_streams.get_list_of_users_stream_ids()


    def get_streams_from_file(self):
        # Read from selected streams files
        self.api_streams.clear_all()
        api_streams_csv_list = []


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
            # Get the streams as CSV from the BT file
            try:
                with open(os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_requests_filename)) \
                        as f_requests:
                    api_streams_csv_list.extend(f_requests.readlines())
            except Exception as err:
                print(
                    'Unable to read BT streams file ' + self.bt_requests_filename + ' file in '
                    + self.restful_bt_sources_dir + '. ' + str(err))

        for stream_params_str in api_streams_csv_list:
            self.api_streams.append_request(stream_params_str, api_key, username)
        api_streams_csv_list.clear()

        # Get the streams as CSV from the Triangulum file
        try:
            with open(os.path.join(
                    self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_requests_filename)) \
                    as f_requests:
                api_streams_csv_list.extend(f_requests.readlines())
        except Exception as err:
            print('Unable to read Triangulum streams file ' + self.triangulum_requests_filename + ' file in '
                  + self.restful_triangulum_sources_dir + '. ' + str(err))

        for stream_params_str in api_streams_csv_list:
            self.api_streams.append_request(stream_params_str)
        api_streams_csv_list.clear()

        try:
            with open(os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_credentials_filename), \
                      "r+") as f_creds:
                cdp_credentials = f_creds.readline().split(',')
                list_params = cdp_credentials[0]
                api_key = list_params.rstrip('\n')
        except Exception as err:
            print('Unable to read CDP credentials file ' + self.cdp_credentials_filename + ' file in '
            + self.restful_cdp_sources_dir + '. ' + str(err))
        else:
            try:
                with open(os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_requests_filename)) \
                        as f_requests:
                    api_streams_csv_list.extend(f_requests.readlines())
            except Exception as err:
                print('Unable to read CDP streams file ' + self.cdp_requests_filename + ' file in '
                    + self.restful_cdp_sources_dir + '. ' + str(err))


        for stream_params_str in api_streams_csv_list:
            self.api_streams.append_request(stream_params_str, api_key)


    def clear_all_streams(self):
        self.api_streams.clear_all()
        new_file = []

        # Empty the request files
        #if os.path.exists(os.path.dirname(os.path.join(self.data_source_dir, self.restful_bt_sources_dir))):
        #    with open(os.path.join(self.data_source_dir, self.restful_bt_sources_dir,
        #                           self.bt_credentials_filename), "w+") as f_out:
        #        f_out.writelines(new_file)

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

        if (hub_url == Data_hub_call_restful_bt.core_URL):  # "http://api.bt-hypercat.com"):
            file_name = os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_requests_filename)
        elif (hub_url == Data_hub_call_osisoft_pi.core_URL):  # "https://130.88.97.137/piwebapi"):
            file_name = os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_requests_filename)
        elif (hub_url == Data_hub_call_cdp.core_URL):
            file_name = os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_requests_filename)
        else:
            raise

        if not os.path.exists(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name), exist_ok=True)

        try:
            with open(file_name, "r+") as fp:
                temp = fp.readlines()
                fp.seek(0)
                for line in temp:
                    if line.strip() != stream_href:
                        fp.write(line)
                fp.truncate()
                fp.close()
        except Exception as err:
            print(
                'Unable to read streams file: ' + file_name + '. ' + str(err))

        self.get_streams_from_file()



    def add_to_streams(self, new_stream_params):
        str_stream_params = json.dumps(new_stream_params)
        hub_url = new_stream_params['stream_params'][0]
        stream_href = new_stream_params['feed_info']['href']

        if (hub_url == Data_hub_call_restful_bt.core_URL):  # "http://api.bt-hypercat.com"):
            file_name = os.path.join(self.data_source_dir, self.restful_bt_sources_dir, self.bt_requests_filename)
        elif (hub_url == Data_hub_call_osisoft_pi.core_URL):  # "https://130.88.97.137/piwebapi"):
            file_name = os.path.join(self.data_source_dir, self.restful_triangulum_sources_dir, self.triangulum_requests_filename)
        elif (hub_url == Data_hub_call_cdp.core_URL):
            file_name = os.path.join(self.data_source_dir, self.cdp_sources_dir, self.cdp_requests_filename)
        else:
            raise

        if not os.path.exists(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name), exist_ok=True)

        try:
            if os.path.exists(file_name):
                append_write = 'r+'  # append if already exists
            else:
                append_write = 'w+'  # make a new file if not

            with open(file_name, append_write) as fp:
                for line in fp:
                    if stream_href in line:
                        break
                else:  # not found, we are at the eof
                    fp.write(str_stream_params+'\n')  # append missing data
                fp.close()
        except Exception as err:
            print(
                'Unable to read streams file: ' + file_name + '. ' + str(err))

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