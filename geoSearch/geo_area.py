import requests
import json
import csv
from utils import write_header_csv
from ripe.atlas.cousteau import AtlasResultsRequest

class GeoArea:
    """
    the class contains geographic coordinates, time interval and methods used for search

    attributes
    # dest
        lat_lte_dst, lat_gte_dst, lon_lte_dst, lon_gte_dst
    # src 
        lat_lte_src, lat_gte_src, lon_lte_src, lon_gte_src
    # Unix timestamp
        start_time, stop_time 
    """
    def __init__(self, lat_lte_dst, lat_gte_dst, lon_lte_dst, lon_gte_dst,
                lat_lte_src, lat_gte_src, lon_lte_src, lon_gte_src, start_time, stop_time):
        
        self.base_url = "https://atlas.ripe.net/api/v2/"
        self.probe_ids_list = []
        self.msm_ids_list = []
    
        self.src_probes = dict()
        self.dest_probes = dict()
        # dest
        self.lat_lte_dst = lat_lte_dst
        self.lat_gte_dst = lat_gte_dst
        self.lon_lte_dst = lon_lte_dst
        self.lon_gte_dst = lon_gte_dst
        # src 
        self.lat_lte_src = lat_lte_src
        self.lat_gte_src = lat_gte_src
        self.lon_lte_src = lon_lte_src
        self.lon_gte_src = lon_gte_src
        # Unix timestamp
        self.start_time = start_time
        self.stop_time = stop_time
            
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def save_probes(self, probes_file, fields, results, is_target = False):
        """
        save in 'probes_file' some data about probes
        """
        #print("save_probes")

        with open(probes_file, 'a') as results_file:
            writer = csv.DictWriter(results_file, fieldnames=fields)

            for res in results['results']:
                # useful data probes
                dict_res = {'prb_id': res['id'], 'address_v4': res['address_v4'], 'address_v6': res['address_v6'], 'asn_v4': res['asn_v4'], 'asn_v6': res['asn_v6']}
                if is_target:
                    dict_res['msm_url'] = res['measurements']

                # saving data to file
                writer.writerow(dict_res)

                if is_target:
                    # for target probes we need the asn corresponding to the ip address

                    if res['address_v4']:
                        self.dest_probes[res['address_v4']] = res['asn_v4']

                    if res['address_v6']:
                        self.dest_probes[res['address_v6']] = res['asn_v6']
                
                else:
                    # for source probes:
                    # save the corresponding id, for afterwards filter the results
                    self.probe_ids_list.append(res['id'])

                    # for each probe id, save the corresponding ip address and asn to use for eda
                    self.src_probes[str(dict_res.pop('prb_id'))] = dict_res
        
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def write_header_msm_list(self, list_msm_file):

        fieldnames = ['msm_id', 'target', 'target_ip', 'msm_type']  #'msm_result',
        write_header_csv(list_msm_file, fieldnames)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def save_msm_list(self, msm_file, results):
        """
        save in 'msm_file' the list of measurements
        """
        #print("save_msm_list")

        with open(msm_file, 'a') as results_file:

            writer = csv.DictWriter(results_file, fieldnames=['msm_id', 'target', 'target_ip', 'msm_type'])
            
            for res in results['results']:
                if res['id'] not in self.msm_ids_list:
                    self.msm_ids_list.append(res['id'])
                        #'msm_result': res['result'], 
                    dict_res = {'msm_id': res['id'], 'target': res['target'], 'target_ip': res['target_ip'], 'msm_type': res['type']}
                    writer.writerow(dict_res)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_dest(self, probes_file, optional_fields = 'measurements'):
        """ get target probes """

        print("get dest")
        self.get_probes(probes_file, self.lat_lte_dst, self.lat_gte_dst, self.lon_lte_dst, self.lon_gte_dst, 
                        is_target = True, optional_fields = optional_fields)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_src(self, probes_file, optional_fields = None):
        """ get source probes """

        print("get source")
        self.get_probes(probes_file, self.lat_lte_src, self.lat_gte_src, self.lon_lte_src, self.lon_gte_src, 
                        is_target = False, optional_fields = optional_fields)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_probes(self, probes_file, lat_lte, lat_gte, lon_lte, lon_gte, is_target = False, optional_fields = None):
        """
        Given latitude and longitude, get all probes in that geograpics area
        is_target - True if destination area
        probes_file - file where to save data probes
        """
        # print("get_probes")

        fieldnames = ['prb_id', 'address_v4', 'address_v6', 'asn_v4', 'asn_v6']
        if is_target:
            fieldnames.append('msm_url') 

        write_header_csv(probes_file, fieldnames)

        parameters = {'format': 'json', 'latitude__lte': lat_lte, 'latitude__gte': lat_gte, 'longitude__lte': lon_lte, 'longitude__gte': lon_gte, 'page_size': 100}
        if optional_fields is not None:
            parameters['optional_fields'] = optional_fields

        try:
            result = requests.get(self.base_url + 'probes/', params=parameters)
            print('probes url', result.url)
            result = result.json()
            self.save_probes(probes_file, fieldnames, result, is_target)

            while result['next']:
                result = requests.get(result['next']).json()
                self.save_probes(probes_file, fieldnames, result, is_target)

        except Exception as ex:
            print('probes request failed', ex)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_msm_list(self, msm_file, type_msm, target, optional_fields = None):
        """
        get measurements of type 'type_msm' that have as destination the ip address specified by 'target'
        start < stop_time & (stop > start_time | status = ongoing)
        'msm_file': file for list of measurements
        """
        print("get_msm_list: ", type_msm, target)

        parameters = {'format': 'json', 'type': type_msm, 'start_time__lte': self.stop_time, 'stop_time__gt': self.start_time,
                    'target_ip': target, 'page_size': 100}
        if optional_fields is not None:
            parameters['optional_fields'] = optional_fields
        
        for i in range(2):

            try:
                msm_list = requests.get(self.base_url + 'measurements/', params=parameters)
                #print('msm url', msm_list.url)
                msm_list = msm_list.json()
                #print(msm_list)
                #print("msm_count", msm_list['count'])
                self.save_msm_list(msm_file, msm_list)

                while msm_list['next']:
                    msm_list = requests.get(msm_list['next']).json()
                    self.save_msm_list(msm_file, msm_list)
            except:
                print("get_msm_list failed", i)

            if i == 0:
                del parameters['stop_time__gt']
                parameters['status'] = 2    #2: Ongoing measurements

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_msm_results(self, results_file, msm_id):
        """
        fetch results for measurement 'msm_id' and for source probes, from start_time to stop_time
        """

        print("get_msm_results", msm_id)

        kwargs = {
            "msm_id": msm_id,
            "start": self.start_time,
            "stop": self.stop_time,
            "probe_ids": self.probe_ids_list
        }

        try:
            is_success, results = AtlasResultsRequest(**kwargs).create()    #from ripe.atlas.cousteau

            if is_success:
                with open(results_file, 'a') as msm_result:
                    for res in results:
                        #print('rtt', PingResult(res).rtt_max)
                        msm_result.write(json.dumps(res) + "\n")
            else:
                print("get_results is not success")
        except:
            print("get_results failed")

__all__ = (
    'GeoArea',
)