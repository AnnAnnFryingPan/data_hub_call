# Polymorphic factory methods.
from __future__ import generators
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi
from data_hub_call_restful_cdp import Data_hub_call_restful_cdp
from data_hub_call_restful_bt import Data_hub_call_restful_bt

class Data_hub_call_factory:
    factories = {}

    def add_factory(id, data_hub_call_factory):
        Data_hub_call_factory.factories.put[id] = data_hub_call_factory
    add_factory = staticmethod(add_factory)

    # A Template Method:
    def create_data_hub_call(request):
        if request.hub_call_classname not in Data_hub_call_factory.factories:
            Data_hub_call_factory.factories[request.hub_call_classname] = eval(request.hub_call_classname + '.Factory()')
        return Data_hub_call_factory.factories[request.hub_call_classname].create(request)

    create_data_hub_call = staticmethod(create_data_hub_call)