from request_info import Request_info
from enum import Enum

class Element_type(Enum):
    __order__ = 'entity'
    none = 0
    entity = 1

class Request_info_restful_cdp(Request_info):
    """A data stream from any restful CDP style platform/hub:
    """

    @staticmethod
    def get_element_types():
        return [(e.value, e.name) for e in Element_type]



    """Attributes:
        api_core_url: The url of the data hub. eg 'http://api.bt-hypercat.com'
        feed_id: The id of the parent feed to which the datastream belongs
        datastream_id: the id of the datastream. Eg. 0, 1, 2...
        feed_type: either 'sensors', 'events', 'locations' or 'geo'
    """

    def __init__(self, api_key, api_core_url, hub_version, element_type, element_id, instance_id,
                 users_feed_name, feed_info):

        super(Request_info_restful_cdp, self).__init__(api_core_url, users_feed_name, feed_info)

        self.api_key = api_key                  # https://api.cityverve.org.uk
        self.hub_version = hub_version          # v1
        self.element_type = element_type        # entity
        self.element_id = element_id            # noise-meter
        self.instance_id = instance_id          # 6472W
        self.users_feed_name = users_feed_name  # My_feed_name
        self.feed_info = feed_info              # {...}





