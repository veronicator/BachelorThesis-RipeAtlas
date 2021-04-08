import os
import requests
import json
import utils
from ripe.atlas.cousteau import AtlasResultsRequest

class GeoArea:
    """
    attributes
    # dest
        lat_lte_dst
        lat_gte_dst
        lon_lte_dst
        lon_gte_dst
    # src 
        lat_lte_src
        lat_gte_src 
        lon_lte_src
        lon_gte_src
    # Unix timestamp
        start_time 
        stop_time 
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
            save in 'probes_file' some infos about probes in 'results'
        """
        #print("save_probes")

        with open(probes_file, 'a') as results_file:
            writer = csv.DictWriter(results_file, fieldnames=fields)

            for res in results['results']:
                #infos needed
                dict_res = {'prb_id': res['id'], 'address_v4': res['address_v4'], 'address_v6': res['address_v6'], 'asn_v4': res['asn_v4'], 'asn_v6': res['asn_v6']}
                if is_target:
                    dict_res['msm_url'] = res['measurements']

                # writing on file
                writer.writerow(dict_res)

                if is_target:

                    if res['address_v4']:
                        dest_probes[res['address_v4']] = res['asn_v4']

                    if res['address_v6']:
                        dest_probes[res['address_v6']] = res['asn_v6']
                
                else:
                    #per le probe sorgenti, salva il relativo id in una lista da utilizzare per filtrare i risultati
                    probe_ids_list.append(res['id'])

                    #salva in memoria indirizzi ip e asn delle probe sorgenti, da utilizzare per l'eda
                    src_probes[str(dict_res.pop('prb_id'))] = dict_res
        
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def save_msm_list(self, msm_file, results):
        """
        salva in 'msm_file' la lista delle measurement contenute in 'results'
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

    def find_dest(self, probes_file, optional_fields = None):
        self.find_probes(probes_file, self.lat_lte_dst, self.lat_gte_dst, self.lon_lte_dst, self.lon_gte_dst, 
                        is_target = True, optional_fields = optional_fields)
        pass

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def find_src(self, probes_file, optional_fields = None):
        self.find_probes(probes_file, self.lat_lte_src, self.lat_gte_src, self.lon_lte_src, self.lon_gte_src, 
                        is_target = False, optional_fields = optional_fields)
        pass

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def find_probes(self, probes_file, lat_lte, lat_gte, lon_lte, lon_gte, is_target = False, optional_fields = None):
        """
        cerca le probe presenti in un'area geografica delimitata dalle latitudini e longitudini indicate,
        is_target - True if destination area
        probes_file - file in cui salvare i dettagli delle probe
        """

        fieldnames = ['prb_id', 'address_v4', 'address_v6', 'asn_v4', 'asn_v6']
        if is_target:
            fieldnames.append('msm_url') 
        #fieldnames = fieldnames + ",msm_url" if is_target else fieldnames
        utils.write_header_csv(probes_file, fieldnames)

        #fieldnames = fieldnames.split(',')

        # print("find_probes")
        parameters = {'format': 'json', 'latitude__lte': lat_lte, 'latitude__gte': lat_gte, 'longitude__lte': lon_lte, 'longitude__gte': lon_gte, 'page_size': 100}
        if optional_fields is not None:
            parameters['optional_fields'] = optional_fields

        try:
            result = requests.get(base_url + 'probes/', params=parameters)
            print('probes url', result.url)
            result = result.json()
            save_probes(probes_file, fieldnames, result, is_target)

            while result['next']:
                result = requests.get(result['next']).json()
                save_probes(probes_file, fieldnames, result, is_target)

        except Exception as ex:
            print('probes request failed', ex)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def find_msm_list(self, msm_file, type_msm, target, optional_fields = None):
        """
        cerca le measurement di tipo 'type_msm' che hanno come destinazione l'indirizzo ip indicato da 'target': 
        start < stop_time & (stop > start_time | status = ongoing)
        'msm_file': file in cui salvare i risultati ottenuti
        """
        print("find_msm_list: ", type_msm, target)

        parameters = {'format': 'json', 'type': type_msm, 'start_time__lte': self.stop_time, 'stop_time__gt': self.start_time,
                    'target_ip': target, 'page_size': 100}
        if optional_fields is not None:
            parameters['optional_fields'] = optional_fields
        
        for i in range(2):

            try:
                msm_list = requests.get(base_url + 'measurements/', params=parameters)
                #print('msm url', msm_list.url)
                msm_list = msm_list.json()
                #print(msm_list)
                #print("msm_count", msm_list['count'])
                save_msm_list(msm_file, msm_list)

                while msm_list['next']:
                    msm_list = requests.get(msm_list['next']).json()
                    save_msm_list(msm_file, msm_list)
            except:
                print("find_msm_list failed", i)

            if i == 0:
                del parameters['stop_time__gt']
                parameters['status'] = 2    #2: Ongoing measurements

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def find_msm_results(self, geo_msm, msm_id, start_time, stop_time):
        """
        ottiene i dati contenuti in 'result_url', filtrando i risultati per probe sorgenti e per timestamp
        'results_file': file in cui salvare i risultati
        'probes_ids_list': lista degli id delle probe nell'area sorgente
        'start_time', 'stop_time': definiscono l'intervallo di tempo di cui si vogliono i risultati    
        """
        #global probe_ids_list

        print("find_msm_results")

        kwargs = {
            "msm_id": msm_id,
            "start": self.start_time,
            "stop": self.stop_time,
            "probe_ids": self.probe_ids_list
        }

        is_success, results = AtlasResultsRequest(**kwargs).create()

        if is_success:
            with open(geo_msm.results_file, 'a') as msm_result:
                for res in results:
                    #print('rtt', PingResult(res).rtt_max)
                    msm_result.write(json.dumps(res) + "\n")
                    #geo_msm.parse_msm(res, src_probes, dest_probes)
        else:
            print("find_results failed")

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
