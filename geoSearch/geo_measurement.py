#%%
import os
import csv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from ripe.atlas.sagan import PingResult

# Comment this if the data visualisations doesn't work on your side
#%matplotlib inline

class GeoMeasurement:
    """ 
    parent class for all measurement type classes

    attributes:
        type_msm
        results_dir = directory name for result files
        plots_dir = path/directory name for eda plot
        list_msm_file = path/filename for list of found measurements
        results_file = path/filename for measurements result
        eda_tab_v4/eda_tab_v6 = tables data for plot
    """

    def __init__(self, type_msm, **kwargs):
        self.type_msm = type_msm
        self.results_dir = kwargs.get("results_dir", "./ripe_geo_results/")
        self.plots_dir = kwargs.get("plots_dir", self.results_dir + "ripe_geo_plots/")
        self.list_msm_file = kwargs.get("list_msm_file",  self.results_dir + "msm_list.csv")
        self.results_file = kwargs.get("results_file", self.results_dir + "msm_results.txt")
        eda_tab_v4 =  self.results_dir + kwargs.get("tab_msm_v4", self.type_msm + "_tab_v4.csv")
        eda_tab_v6 =  self.results_dir + kwargs.get("tab_msm_v6", self.type_msm + "_tab_v6.csv")
        self.eda_tab = {self.type_msm + '_v4': eda_tab_v4, self.type_msm + '_v6': eda_tab_v6}
        self.msm_v4 = []
        self.msm_v6 = []

        if not os.path.exists(self.results_dir):
            os.mkdir(self.results_dir)

        if not os.path.exists(self.plots_dir):
            os.mkdir(self.plots_dir)
    
    def parse_msm(self, src_probes, dest_probes, **kwargs):
        print(self.type_msm, "parse not implemented yet")
        pass

    def write_tab_result(self, fields, msm_result, eda_tab):
        """ write to csv file the filter data to plot """

        with open(eda_tab, 'w') as csvf:
            print('open tab msm results')

            writer = csv.DictWriter(csvf, fieldnames=fields)
            writer.writeheader()

            for res in msm_result:
                writer.writerow(res) 
    
    def eda_plot_results(self, dataframe, x_data, y_data, x_name, y_name, name_figure, hue=None, row=None, col=None, palette="deep", 
                        legend='auto', kind_plot='scatter', height=10, aspect=1, dot_size=7):
        """ drawing relational scatter plots """
        
        sns.set_theme()

        plot = sns.relplot(data=dataframe, x=x_data, y=y_data, col=col, kind=kind_plot, palette=palette, 
                            hue=hue, legend=legend, s=dot_size, height=height, aspect=aspect)

        plot.set_axis_labels(x_name, y_name)
        #plt.legend(loc="center left", bbox_to_anchor=(1.2, 1), labels="")
        plt.tight_layout()
        plt.savefig(name_figure)
        plt.figure()
    
    def eda_msm_result(self):
        print(self.type_msm, "eda not implemented yet")
        pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class GeoPing(GeoMeasurement):
    """ 
    ping measurements class

    attributes:
        type_msm = "ping"
        results_dir = directory name for result files
        plots_dir = directory name for eda plot
        list_msm_file = path/filename for list of found measurements
        results_file = path/filename for measurements result
    """

    def __init__(self, type_msm="ping", **kwargs):
        super().__init__(type_msm, tab_msm_v4="ping_tab_v4.csv", tab_msm_v6="ping_tab_v6.csv", **kwargs)

    def parse_msm(self, src_probes, dest_probes, **kwargs):
        """
            parse 'result' and append it to msm list
        """
        print('parse_ping')

        with open(self.results_file) as results:
            # read file of results for parsing
            for res in results.readlines():

                parsed_result = PingResult(res)
                af = str(parsed_result.af)
                print("origin", parsed_result.origin)
                print("address", src_probes[str(parsed_result.probe_id)]['address_v' + af])

                if parsed_result.origin == src_probes[str(parsed_result.probe_id)]['address_v' + af]:
                    
                    # only results whose source address matches the current address 
                    # of the corresponding source probe are selected,
                    # all other results are discarded
                    
                    ping_res = {'af': parsed_result.af, 
                                'ip_src': parsed_result.origin, 
                                'ip_dest': parsed_result.destination_address, 
                                'asn_src': src_probes[str(parsed_result.probe_id)]['asn_v' + af], 
                                'asn_dest': dest_probes[parsed_result.destination_address], 
                                'timestamp': parsed_result.created_timestamp, 
                                'rtt_min': parsed_result.rtt_min}
                    
                    self.msm_v4.append(ping_res) if parsed_result.af == 4 else self.msm_v6.append(ping_res)
        
        self.write_tab_result()

    def write_tab_result(self):

        fields = ['af','ip_src','ip_dest','asn_src','asn_dest','timestamp','rtt_min']

        super().write_tab_result(fields, self.msm_v4, self.eda_tab['ping_v4'])
        # super().write_tab_result(fields, self.msm_v4, self.eda_tab_v4)

        super().write_tab_result(fields, self.msm_v6, self.eda_tab['ping_v6'])
        # super().write_tab_result(fields, self.msm_v6, self.eda_tab_v6)

    def eda_plot_results(self, df_ping, type_af, **kwargs):

        df_ping['timestamp'] = pd.to_datetime(arg=df_ping['timestamp'], unit='s')

        super().eda_plot_results(df_ping, x_data="timestamp", y_data="rtt_min", x_name="Time (UTC)", y_name="RTT min (ms)", kind_plot='scatter',
                                height=7, aspect=1.5, dot_size=10, name_figure=self.plots_dir + type_af + "_rtt.png")

        super().eda_plot_results(df_ping, x_data="timestamp", y_data="rtt_min", x_name="Time (UTC)", y_name="RTT min (ms)", col="asn_src", 
                                hue="asn_dest", palette="deep", legend='full', kind_plot='scatter', height=7, aspect=1.5, dot_size=10, 
                                name_figure=self.plots_dir + type_af + "_asn_src.png")

        super().eda_plot_results(df_ping, x_data="timestamp", y_data="rtt_min", x_name="Time (UTC)", y_name="RTT min (ms)", col="asn_dest", 
                                hue="asn_src", palette="deep", legend='full', kind_plot='scatter', height=7, aspect=1.5, dot_size=10, 
                                name_figure=self.plots_dir + type_af + "_asn_dest.png")
        
    def eda_msm_result(self):
        print("eda ping")
        for tab in self.eda_tab:
            df_ping = pd.read_csv(eda_tab[tab])
            print(tab)
            print(df_ping.head())
            print("describe rtt_min", tab, "\n", df_ping['rtt_min'].describe())
            print('asn_src:', df_ping['asn_src'].nunique()) # Count distinct observations over requested axis.
            print('asn_dest:', df_ping['asn_dest'].nunique())

            self.eda_plot_results(df_ping, tab)

        """
        df_ping = pd.read_csv(self.eda_tab_v4)    #return pandas.DataFrame
        print(df_ping.head())     #.tail(n=row to select)/.head(n)
        #print("get", df_ping_4.get('asn_src'))
        print("describe rtt_min v4\n", df_ping['rtt_min'].describe())
        print('asn_src_v4:', df_ping['asn_src'].nunique()) # Count distinct observations over requested axis.
        print('asn_dest_v4:', df_ping['asn_dest'].nunique())

        self.eda_plot_results(df_ping, "ping_v4")

        df_ping = pd.read_csv(self.eda_tab_v6)    #return pandas.DataFrame
        print(df_ping.head())
        print("describe rtt_min v6\n", df_ping['rtt_min'].describe())
        print('asn_src_v6:', df_ping['asn_src'].nunique()) # Count distinct observations over requested axis.
        print('asn_dest_v6:', df_ping['asn_dest'].nunique())

        self.eda_plot_results(df_ping, "ping_v6")
        """

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    

class GeoTraceroute(GeoMeasurement):
    def __init__(self, type_msm="traceroute", **kwargs):
        super().__init__(type_msm, **kwargs)

__all__ = (
    'GeoMeasurement',
    'GeoPing',
    'GeoTraceroute'
)