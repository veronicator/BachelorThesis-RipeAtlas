import os
import csv
import argparse
import geo_measurement as geo
from datetime import datetime
from geo_area import GeoArea
from utils import coordinates_range

### Global ###

results_dir = './ripe_geo_results/'
plots_dir = results_dir + 'ripe_geo_plots/'

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

    area = GeoArea(lat_lte_dst, lat_gte_dst, lon_lte_dst, lon_gte_dst,
                lat_lte_src, lat_gte_src, lon_lte_src, lon_gte_src, start_time, stop_time)

    if not os.path.exists(results_dir):
        os.mkdir(results_dir)

    if not os.path.exists(plots_dir):
        os.mkdir(plots_dir)

    src_file = 'src_probes.csv'
    dest_file = 'dest_probes.csv'

    target_ip_type = ['address_v4', 'address_v6']
 
    geo_msm_list = [geo.GeoPing(type_msm="ping", results_dir=results_dir, plots_dir=plots_dir, 
                    list_msm_file=results_dir + "ping_list.csv", results_file=results_dir + "ping_results.txt"),
                geo.GeoTraceroute(type_msm="traceroute", results_dir=results_dir, plots_dir=plots_dir, 
                    list_msm_file=results_dir + "traceroute_list.csv", results_file=results_dir + "traceroute_results.txt")]

    area.find_dest(dest_file, optional_fields='measurements')
    area.find_src(src_file)
    
    #find_probes(dest_file, lat_lte_dst, lat_gte_dst, lon_lte_dst, lon_gte_dst, is_target=True, optional_fields='measurements')
    #find_probes(src_file, lat_lte_src, lat_gte_src, lon_lte_src, lon_gte_src)

    #probe_ids = join_list(probe_ids_list)
    print("probe id", area.probe_ids_list)
    #print('src_probe', src_probes)
    #print('dest_probe', dest_probes)

    #header measurements list file
    #fieldnames = ['msm_id', 'target', 'target_ip', 'msm_type']  #'msm_result',

    for msm in geo_msm_list:
        area.write_header_msm_list(msm.list_msm_file)
        #write_header_csv(msm.list_msm_file, fieldnames)

    with open(dest_file) as csvf:
        """
        per ogni nodo destinazione in dest_file cerca le measurements 
        il cui target corrisponde all'indirizzo ip del nodo stesso
        """        
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            for type_ip in target_ip_type:
                if row[type_ip]:
                    for msm in geo_msm_list:
                        area.find_msm_list(msm.list_msm_file, msm.type_msm, row[type_ip])
                        #find_msm_list(msm.list_msm_file, msm.type_msm, row[type_ip], start_time, stop_time)


    for msm in geo_msm_list:
        """
        per ogni tipo di measurement, scorre la lista delle measurement precedentemente trovate e 
        per ognuna cerca gli eventuali risultati delle misure che partono dall'area sorgente verso
        l'area destinazione scelte
        """        
        open(msm.results_file, 'w').close()  # create or drop an existing file

        with open(msm.list_msm_file) as csvf:
            csvReader = csv.DictReader(csvf)

            for row in csvReader:
                area.find_msm_results(msm.results_file, row['msm_id'], start_time, stop_time)
                #find_msm_results(msm, row['msm_id'], start_time, stop_time)

    for geo_msm in geo_msm_list:
        print('msm in', msm)
        #if isinstance(geo_msm, geo.GeoPing):
        geo_msm.parse_msm(area.src_probes, area.dest_probes)
        #geo_msm.write_tab_result()
        geo_msm.eda_msm_result()
        #else:
        #    print("others type eda not implemented yet")
    
if __name__ == '__main__':
    main()