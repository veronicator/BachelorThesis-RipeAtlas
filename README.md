## tesiRipe
***
# geo searching for measurements on Ripe Atlas


The way to use/test the script is to run "ripe_geo_search.py" with some arguments.
Run the script, into his directory, as follow:

```
./ripe_geo_search.py 9 49.5 11 11.5 38.5 39 -77.5 -77 2020-10-07\ 05:00:00 2020-10-07\ 12:00:00 
```
Positional arguments:
  lat_min_dest    destination lower latitude (DD)
  lat_max_dest    destination higher latitude (DD)
  lon_min_dest    destination lower longitude (DD)
  lon_max_dest    destination higher longitude (DD)
  lat_min_src     source lower latitude (DD)
  lat_max_src     source higher latitude (DD)
  lon_min_src     source lower longitude (DD)
  lon_max_src     source higher longitude (DD)
  start_datetime  start datetime in ISO format, i.e. 2021-03-24\ 18:05:00
  stop_datetime   stop datetime in ISO format, i.e. 2021-03-25\ 08:25:05


The scripts depend on this convention.

Dependencies on external python modules are in 'requirements.txt',
so you can run

```shell
pip install -r requirements.txt
```

The script create a folder named "ripe_geo_results" in which you can find, at the end of running, 
the file of source and destination probes, measurements list and results, 
and a subfolder "ripe_geo_plots" with the png plots of ping rtt minimum
