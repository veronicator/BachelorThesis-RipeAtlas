import csv

def write_header_csv(filename, fields):

    with open(filename, 'w') as new_file:
        csv.DictWriter(new_file, fieldnames=fields).writeheader()
        
#------------------------------------------------------------