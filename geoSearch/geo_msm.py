#%%
import csv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from ripe.atlas.sagan import PingResult, TracerouteResult

class GeoMeasurement:

    def __init__(self, **kwargs):
        self.results_dir = kwargs.get("results_dir", "./")
        self.type_msm = kwargs.get("type_msm")
        self.list_file = kwargs.get("list_file", "msm_list.csv")
        self.results_file = kwargs.get("results_file", self.results_dir + "msm_results.txt")
        self.eda_tab_v4 = kwargs.get("tab_msm_v4", "tab_msm_v4.csv")
        self.eda_tab_v6 = kwargs.get("tab_msm_v6", self.results_dir + "tab_msm_v6.csv")
        self.msm_v4 = []
        self.msm_v6 = []


class GeoPing(geoMeasurement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_msm(self, result):
        """
            parse 'result' and append it to msm list
        """
        print('parse_ping')

        parsed_result = PingResult(result)
        af = str(parsed_result.af)
        print("origin", parsed_result.origin)
        print("address", src_probes[str(parsed_result.probe_id)]['address_v' + af])

        if parsed_result.origin == src_probes[str(parsed_result.probe_id)]['address_v' + af]:
            
            # vengono selezionati soltanti i risultati il cui indirizzo sorgente 
            # corrisponde all'attuale indirizzo della relativa probe,                            
            # tutti gli altri vengono scartati
            
            ping_res = {'af': parsed_result.af, 'ip_src': parsed_result.origin, 'ip_dest': parsed_result.destination_address, 
                'asn_src': src_probes[str(parsed_result.probe_id)]['asn_v' + af], 'asn_dest': dest_probes[parsed_result.destination_address], 
                'timestamp': parsed_result.created_timestamp, 'rtt_min': parsed_result.rtt_min}
            
            self.msm_v4.append(ping_res) if parsed_result.af == 4 else self.msm_v6.append(ping_res)
        
    def write_tab_result(self):
        #for res in ping_v4:
        #    print('v4', res)
        #for res in ping_v6:
        #    print('v6', res)

        fields = ['af','ip_src','ip_dest','asn_src','asn_dest','timestamp','rtt_min']

        with open(self.eda_tab_v4, 'w') as csvf:
            print('open tab ping v4')

            writer = csv.DictWriter(csvf, fieldnames=fields)
            writer.writeheader()

            for res in self.msm_v4:
                writer.writerow(res) 

        with open(self.eda_tab_v6, 'w') as csvf:
            print('open tab ping v6')

            writer = csv.DictWriter(csvf, fieldnames=fields)
            writer.writeheader()

            for res in self.msm_v6:
                writer.writerow(res)

    def plot_msm(self):

__all__ = (
    'GeoMeasurement',
    'GeoPing',
)