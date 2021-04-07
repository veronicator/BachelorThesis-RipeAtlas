#%%
import os
import csv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from ripe.atlas.sagan import PingResult, TracerouteResult

# Comment this if the data visualisations doesn't work on your side
#%matplotlib inline

class GeoMeasurement:

    def __init__(self, **kwargs):
        self.results_dir = kwargs.get("results_dir", "../geo_results")
        self.plots_dir = kwargs.get("plots_dir", "../geo_plots")
        self.type_msm = kwargs.get("type_msm")
        self.list_file = kwargs.get("list_file", "msm_list.csv")
        self.results_file = kwargs.get("results_file", self.results_dir + "msm_results.txt")
        self.eda_tab_v4 = kwargs.get("tab_msm_v4", "tab_msm_v4.csv")
        self.eda_tab_v6 = kwargs.get("tab_msm_v6", self.results_dir + "tab_msm_v6.csv")
        self.msm_v4 = []
        self.msm_v6 = []

        if not os.path.exists(self.results_dir):
            os.mkdir(results_dir)

        if not os.path.exists(self.plots_dir):
            os.mkdir(plots_dir)
    
    def parse_msm(self, result):
        pass

    def write_tab_result(self, fields, msm_result, eda_tab):

        with open(eda_tab, 'w') as csvf:
            print('open tab msm results')

            writer = csv.DictWriter(csvf, fieldnames=fields)
            writer.writeheader()

            for res in msm_result:
                writer.writerow(res) 
    
    def eda_plot_results(self, dataframe, x_data, y_data, hue=None, row=None, col=None, palette="deep", 
                        legend='auto', kind='scatter', height=10, aspect=1, dot_size=7, name_figure):
        
        sns.set_theme()

        plot = sns.relplot(
                            data=dataframe, x=x_data, y=y_data, col=col, 
                            kind=kind_plot, palette=palette, hue=hue, 
                            legend=legend, s=dot_size, height=height, aspect=aspect
        )

        plot.set_axis_labels('Time (GMT)', 'RTT min (ms)')
        plt.savefig(name_figure)
        plt.figure()


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

        fields = ['af','ip_src','ip_dest','asn_src','asn_dest','timestamp','rtt_min']

        GeoMeasurement.write_tab_result(self, fields, self.msm_v4, self.eda_tab_v4)

        GeoMeasurement.write_tab_result(self, fields, self.msm_v6, self.eda_tab_v6)

        """
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
        """

    def eda_plot_results(self, df_ping, type_af):

        df_ping['timestamp'] = pd.to_datetime(arg=df_ping['timestamp'], unit='s')
        #print(df_ping_4.head())

        super().eda_plot_results(df_ping, x_data="timestamp", y_data="rtt_min", kind='scatter',
                                height=10, aspect=1, dot_size=7, name_figure=self.plots_dir + type_af + "_rtt.png")

        super().eda_plot_results(df_ping, x_data="timestamp", y_data="rtt_min", col="asn_src", hue="asn_dest", palette="deep", 
                        legend='full', kind='scatter', height=10, aspect=1, dot_size=7, name_figure=self.plots_dir + type_af + "_asn_src.png")

        super().eda_plot_results(df_ping, x_data="timestamp", y_data="rtt_min", col="asn_dest", hue="asn_src", palette="deep", 
                        legend='full', kind='scatter', height=10, aspect=1, dot_size=7, name_figure=self.plots_dir + type_af + "_asn_dest.png")
        
    def eda_ping_result(self):
        df_ping = pd.read_csv(self.eda_tab_v4)    #return pandas.DataFrame
        print(df_ping.head())     #.tail(n=row to select)/.head(n)
        #print("get", df_ping_4.get('asn_src'))
        print("desc", df_ping['rtt_min'].describe())
        print('num asn_src_v4:', df_ping['asn_src'].nunique()) # Count distinct observations over requested axis.
        print('num asn_dest_v4:', df_ping['asn_dest'].nunique())

        self.eda_plot_results(df_ping, "ping_v4")

        df_ping = pd.read_csv(self.eda_tab_v6)    #return pandas.DataFrame
        print(df_ping.head())
        print("desc", df_ping['rtt_min'].describe())
        print('num asn_src_v6:', df_ping['asn_src'].nunique()) # Count distinct observations over requested axis.
        print('num asn_dest_v6:', df_ping['asn_dest'].nunique())

        self.eda_plot_results(df_ping, "ping_v6")

    
class GeoTraceroute(geoMeasurement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

__all__ = (
    'GeoMeasurement',
    'GeoPing',
    'GeoTraceroute'
)