import datetime
import re
from dateutil.parser import parse


class Data_hub_call(object):
    """A data hub call:
    """

    def get_date_time(self, datetime_str):
        # "2017-10-11T11:26:05Z" (Triangulum/Pi recorded data style)
        # "2017-10-11T11:34:09.2461304Z" (Triangulum/Pi interpolated data style)
        # "Thu, 12 Oct 2017 14:30:05 GMT"(BT Datahub style)

        ######### Truncation ###########
        # remove trailing Z
        datetime_str = str(datetime_str).rstrip('Z')
        # if it is a triangulum style date and there are too many digits in last (millisecs) field, then truncate
        if(re.match("^((\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{7,}))$", datetime_str)):
            datetime_str = datetime_str[0:26]


        ########## Matching ###########
        try:

            if (re.match("^((\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{6}))$", datetime_str)):
                # EG: "2017-10-11T11:26:05.246130" (Triangulum/Pi 'interpolated' data style - after truncation)
                result = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%f')
            elif (re.match("^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$", datetime_str)):
                # EG: "2017-10-11T11:26:05" (Triangulum/Pi 'recorded' data style - after truncation)
                result = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
            elif (re.match("^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$", datetime_str)):
                result = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M')
            elif(re.match("^(\w{3}), (\d{2}) (\w{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$", datetime_str)):
                # EG: "Thu, 12 Oct 2017 14:30:05 GMT" (BT Data hub style)
                result = datetime.datetime.strptime(datetime_str, '%a, %d %b %Y %H:%M:%S %Z')
            elif (re.match("^(\w{3}) (\d{2}) (\w{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$", datetime_str)):
                # EG: "Thu 12 Oct 2017 14:30:05 GMT"
                result = datetime.datetime.strptime(datetime_str, '%a %d %b %Y %H:%M:%S %Z')
            elif (re.match("^(\w{3}), (\d{2}) (\w{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2})$", datetime_str)):
                # EG: "Thu, 12 Oct 2017 14:30:05"
                result = datetime.datetime.strptime(datetime_str, '%a, %d %b %Y %H:%M:%S')
            elif (re.match("^(\w{3}) (\d{2}) (\w{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2})$", datetime_str)):
                # EG: "Thu 12 Oct 2017 14:30:05"
                result = datetime.datetime.strptime(datetime_str, '%a %d %b %Y %H:%M:%S')
            else:
                result = parse(datetime_str)
        except:
            raise ValueError('Unable to format datetime from given string: ' + datetime_str)

        return result
