import os
import requests
import json
import csv
import argparse
from datetime import datetime
from ripe.atlas.cousteau import AtlasResultsRequest
from geo_msm import GeoPing

### Global ###
base_url = "https://atlas.ripe.net/api/v2/"

results_dir = '../ripe_geo_results/'
plots_dir = results_dir + 'eda_plot_results/'

probe_ids_list = []
msm_ids_list = []

src_probes = dict()
dest_probes = dict()

#------------------------------------------------------------

def write_header_csv(filename, fields):

    with open(filename, 'w') as new_file:
        csv.DictWriter(new_file, fieldnames=fields).writeheader()
        
#------------------------------------------------------------

def save_probes(probes_file, fields, results, is_target = False):
    """
        save in 'probes_file' some infos about probes in 'results'
    """
    #print("save_probes")
    global probe_ids_list
    global src_probes
    global dest_probes

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
    
#------------------------------------------------------------

def save_msm_list(msm_file, results):
    """
    salva in 'msm_file' la lista delle measurement contenute in 'results'
    """
    global msm_ids_list
    #print("save_msm_list")
    #with open(filename, 'w') as results_file:
    #    writer = csv.DictWriter(results_file, fieldnames=['msm_id', 'msm_result', 'target', 'target_ip', 'msm_type'])
    #    writer.writerow(dict_res)

    with open(msm_file, 'a') as results_file:
        
        for res in results['results']:
            if res['id'] not in msm_ids_list:
                msm_ids_list.append(res['id'])
                #dict_res = {'msm_id': res['id'], 'msm_result': res['result'], 'target': res['target'], 'target_ip': res['target_ip'], 'msm_type': res['type']}
                #
                str_res = str(res['id']) + "," + str(res['result']) + "," + str(res['target']) + "," + str(res['target_ip']) + "," + str(res['type']) + ";\n"
                        
                results_file.write(str_res)

#------------------------------------------------------------

def find_probes(probes_file, lat_lte, lat_gte, lon_lte, lon_gte, is_target = False, optional_fields = None):
    """
    cerca le probe presenti in un'area geografica delimitata dalle latitudini e longitudini indicate,
    is_target - True if destination area
    probes_file - file in cui salvare i dettagli delle probe
    """

    fieldnames = ['prb_id', 'address_v4', 'address_v6', 'asn_v4,asn_v6']
    if is_target:
        fieldnames.append('msm_url') 
    #fieldnames = fieldnames + ",msm_url" if is_target else fieldnames
    write_header_csv(probes_file, fieldnames)

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

#------------------------------------------------------------

def find_msm_list(msm_file, type_msm, target, start_time, stop_time, optional_fields = None):
    """
    cerca le measurement di tipo 'type_msm' che hanno come destinazione l'indirizzo ip indicato da 'target': 
    start < stop_time & (stop > start_time | status = ongoing)
    'msm_file': file in cui salvare i risultati ottenuti
    """
    print("find_msm_list: ", type_msm, target)

    parameters = {'format': 'json', 'type': type_msm, 'start_time__lte': stop_time, 'stop_time__gt': start_time,
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

#------------------------------------------------------------

def find_msm_results(results_file, msm_id, start_time, stop_time):
    """
    ottiene i dati contenuti in 'result_url', filtrando i risultati per probe sorgenti e per timestamp
    'results_file': file in cui salvare i risultati
    'probes_ids_list': lista degli id delle probe nell'area sorgente
    'start_time', 'stop_time': definiscono l'intervallo di tempo di cui si vogliono i risultati    
    """
    global probe_ids_list

    print("find_msm_results")

    parameters = {'probe_ids': probes_ids_list, 'start': start_time, 'stop': stop_time}

    try:
        results = requests.get(result_url, params=parameters)
        #print('result url', results.url)
        results = results.json()

        with open(results_file, 'a') as msm_result:
            for res in results:
                msm_result.write(json.dumps(res) + "\n")
                
    except:
        print("find_results failed")

    kwargs = {
        "msm_id": msm_id,
        "start": start_time,
        "stop": stop_time,
        "probe_ids": probe_ids_list
    }

    is_success, results = AtlasResultsRequest(**kwargs).create()

    if is_success:
        with open(results_file, 'a') as msm_result:
            for res in results:
                print('rtt', PingResult(res).rtt_max)
                msm_result.write(json.dumps(res) + "\n")
    else:
        print("find_results failed")

#------------------------------------------------------------

def parse_ping(results_file, tab_ping_v4, tab_ping_v6):
    """
        eda ping
    """
    print('parse_ping')
    
    ping_v4 = []
    ping_v6 = []

    with open(results_file) as results:
        print('open results_file')

        for res in results.readlines():
            parsed_result = PingResult(res)
            af = str(parsed_result.af)
            print("origin", parsed_result.origin)
            print("address", src_probes[str(parsed_result.probe_id)]['address_v' + af])

            if parsed_result.origin == src_probes[str(parsed_result.probe_id)]['address_v' + af]:
                
                #    vengono selezionati soltanti i risultati il cui indirizzo sorgente 
                #    corrisponde all'attuale indirizzo della relativa probe,                            
                #    tutti gli altri vengono scartati
                
                ping_res = {'af': parsed_result.af, 'ip_src': parsed_result.origin, 'ip_dest': parsed_result.destination_address, 
                    'asn_src': src_probes[str(parsed_result.probe_id)]['asn_v' + af], 'asn_dest': dest_probes[parsed_result.destination_address], 
                    'timestamp': parsed_result.created_timestamp, 'rtt_min': parsed_result.rtt_min}
                
                ping_v4.append(ping_res) if parsed_result.af == 4 else ping_v6.append(ping_res)
    
    #for res in ping_v4:
    #    print('v4', res)
    #for res in ping_v6:
    #    print('v6', res)

    fields = ['af','ip_src','ip_dest','asn_src','asn_dest','timestamp','rtt_min']

    with open(tab_ping_v4, 'w') as csvf:
        print('open tab ping v4')

        writer = csv.DictWriter(csvf, fieldnames=fields)
        writer.writeheader()

        for res in ping_v4:
            writer.writerow(res) 

    with open(tab_ping_v6, 'w') as csvf:
        print('open tab ping v6')

        writer = csv.DictWriter(csvf, fieldnames=fields)
        writer.writeheader()

        for res in ping_v6:
            writer.writerow(res)

#------------------------------------------------------------
def plot_rtt():
    pass
#------------------------------------------------------------

def coordinates_range(mini, maxi):
    """Return function handle of an argument type function for 
       ArgumentParser checking a float range: mini <= arg <= maxi
         mini - maximum acceptable argument
         maxi - maximum acceptable argument"""

    # Define the function with default arguments
    def float_range_checker(arg):
        """New Type function for argparse - a float within predefined range."""

        try:
            f = float(arg)
        except ValueError:    
            raise argparse.ArgumentTypeError("must be a floating point number")
        if f < mini or f > maxi:
            raise argparse.ArgumentTypeError("must be in range [" + str(mini) + " .. " + str(maxi)+"]")
        return f

    # Return function handle to checking function
    return float_range_checker


######### Main ########
def main():

    # required arguments
    parser = argparse.ArgumentParser(description="search measurements between two areas")

    # /home/veronica/Documents/test/env/bin/python /home/veronica/Documents/test/tesi/ripe_geo_search.py
    # 28 29 77 78 48 49 7 8 2013-11-29\ 08:59:00 2013-11-30\ 00:00:00
    # 9 49.5 11 11.5 38.5 39 -77.5 -77 2020-10-07\ 00:00:00 2020-10-07\ 01:00:00 

    # dest arguments
    parser.add_argument("lat_min_dest", help="destination lower latitude (DD)", type=coordinates_range(-90, 90))
    parser.add_argument("lat_max_dest", help="destination higher latitude (DD)", type=coordinates_range(-90, 90))
    parser.add_argument("lon_min_dest", help="destination lower longitude (DD)", type=coordinates_range(-180, 180))
    parser.add_argument("lon_max_dest", help="destination higher longitude (DD)", type=coordinates_range(-180, 180))

    # src arguments
    parser.add_argument("lat_min_src", help="source lower latitude (DD)", type=coordinates_range(-90, 90))
    parser.add_argument("lat_max_src", help="source higher latitude (DD)", type=coordinates_range(-90, 90))
    parser.add_argument("lon_min_src", help="source lower longitude (DD)", type=coordinates_range(-180, 180))
    parser.add_argument("lon_max_src", help="source higher longitude (DD)", type=coordinates_range(-180, 180))

    # interval arguments
    parser.add_argument("start_datetime", help="start datetime in ISO format, i.e. 2021-03-24\ 18:05:00", type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))
    parser.add_argument("stop_datetime", help="stop datetime in ISO format, i.e. 2021-03-25\ 08:25:05", type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))
    
    args = parser.parse_args()
    #print(args)
    
    # dest
    lat_lte_dst = args.lat_max_dest
    lat_gte_dst = args.lat_min_dest
    lon_lte_dst = args.lon_max_dest
    lon_gte_dst = args.lon_min_dest
    # src 
    lat_lte_src = args.lat_max_src
    lat_gte_src = args.lat_min_src
    lon_lte_src = args.lon_max_src
    lon_gte_src = args.lon_min_src

    start_time = args.start_datetime.timestamp().__int__()
    stop_time = args.stop_datetime.timestamp().__int__()
    
    if any([lat_gte_dst > lat_lte_dst, lon_gte_dst > lon_lte_dst, lat_gte_src > lat_lte_src, lon_gte_src > lon_lte_src, start_time > stop_time]):
        print("Invalid arguments")
        return

    if not os.path.exists(results_dir):
        os.mkdir(results_dir)

    if not os.path.exists(plots_dir):
        os.mkdir(plots_dir)

    src_file = 'src_probes.csv'
    dest_file = 'dest_probes.csv'

    target_ip_type = ['address_v4', 'address_v6']

    msm_infos = [{'type_msm': 'ping', 'list_file': 'ping_list.csv', 'results_file': results_dir + 'ping_results.txt'}, 
        {'type_msm': 'traceroute', 'list_file': 'traceroute_list.csv', 'results_file': results_dir + 'traceroute_results.txt'}]

    eda_tab = {'ping': {'v4': results_dir + 'ping_tab_4.csv', 'v6': results_dir + 'ping_tab_6.csv'}}

    find_probes(dest_file, lat_lte_dst, lat_gte_dst, lon_lte_dst, lon_gte_dst, is_target=True, optional_fields='measurements')
    find_probes(src_file, lat_lte_src, lat_gte_src, lon_lte_src, lon_gte_src)

    #probe_ids = join_list(probe_ids_list)
    print("probe id", probe_ids_list)
    #print('src_probe', src_probes)
    #print('dest_probe', dest_probes)

    #header measurements list file
    fieldnames = ['msm_id', 'msm_result', 'target', 'target_ip', 'msm_type']

    for msm in msm_infos:
        write_header_csv(msm['list_file'], fieldnames)

    with open(dest_file) as csvf:
        """
        per ogni nodo destinazione in dest_file cerca le measurements 
        il cui target corrisponde all'indirizzo ip del nodo stesso
        """        
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            for type_ip in target_ip_type:
                if row[type_ip]:    # != 'None':
                    for msm in msm_infos:
                        find_msm_list(msm['list_file'], msm['type_msm'], row[type_ip], start_time, stop_time)


    for msm in msm_infos:
        """
        per ogni tipo di measurement, scorre la lista delle measurement precedentemente trovate e 
        per ognuna cerca gli eventuali risultati delle misure che partono dall'area sorgente verso
        l'area destinazione precedentemente selezionate
        """        
        open(msm['results_file'], 'w').close()  # create or drop an existing file

        with open(msm['list_file']) as csvf:
            csvReader = csv.DictReader(csvf)

            for row in csvReader:
                find_msm_results(msm['results_file'], row['msm_id'], start_time, stop_time)

    for msm in msm_infos:
        print('msm in', msm)
        if msm['type_msm'] == 'ping':
            print('type_ping')
            parse_ping(msm['results_file'], eda_tab['ping']['v4'], eda_tab['ping']['v6'])
        elif msm['type_msm'] == 'traceroute':
            print('type_traceroute: eda not implemented yet')
    
if __name__ == '__main__':
    main()