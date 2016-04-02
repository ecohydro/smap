"""
Process soil moisture data from SMAP installations.

This script belongs in KenyaLab/Data/Tower/TowerData/SMAP and is used to
create properly format *NEW* SMAP files for
transfer. This includes a temperature correction.
The script takes (smapdata2) as input and return individual station
files as output.

This version is written for the new soil moisture installation corresponding
to CR216_SN22028_soil.dat.
The other new installations (e.g.,CR216_SN220XX_soil.dat ) will
need separate copies of this script once it is
up and working.

Hilary Wayland
2015-06-29


"""

import csv
import time


def make_row(line):
    """Make the row."""
    row = []
    row.extend([
        line['Year'], line['Month'], line['DOM'], line['Hour'],
        line['Minute']
    ])
    time_of_year = ((float(line['Hour']) + float(line['Minute']) / 60) / 24 +
                    float(line['Day_of_Year']))
    row.append(str('{0:.2f}').format(time_of_year))
    return row


def make_header():
    """Make the header."""
    header = []
    header.extend([
        'ID', 'TIMESTAMP', 'Tair', 'Prec',
        'SM005', 'TS005', 'SM020', 'TS020'])
    return header


# DATE = datetime.date.today().__str__()
def make_smap_data_for_basin_sites():
    """Make SMAP data files for each basin site."""
    SITE = '2401'

    sites = {
        'Euphorbia': {
            'num': '005',
            'writer': None,
            'SM_FILE': 'smapdata2',
            'header': 'Mpala Research Center, Soil Moisture Station 5 (Euphor)'
        },
        'Open': {
            'num': '006',
            'writer': None,
            'SM_FILE': 'smapdata3',
            'header': 'Mpala Research Center, Soil Moisture Station 6 (Open2)'
        },
        'River': {
            'num': '007',
            'writer': None,
            'SM_FILE': 'smapdata4',
            'header': 'Mpala Research Center, Soil Moisture Station 7 (River)'
        },
        'Glade': {
            'num': '008',
            'writer': None,
            'SM_FILE': 'smapdata5',
            'header': 'Mpala Research Center, Soil Moisture Station 8 (Glade)'
        }
    }
    files_for_upload = []
    for site in sites.keys():
        # Determine the year, month, and day based on last line of infile:
        with open(sites[site]['SM_FILE'], 'rb') as infile:
            import shlex
            # Determine the year, month, and day based on last line of infile:
            last_line = infile.readlines()[-1:]
            # assume timestamp is the first column
            # use shlex to get rid of double quotes in time stamp string
            time_stamp = shlex.split(last_line[0])[0].split()[0]
            SITE_FILE = SITE + sites[site]['num'] + '_' + ''.join(
                time_stamp[:10].split('-')) + '.txt'
        files_for_upload.append(SITE_FILE)
        sitefile = open(SITE_FILE, 'wb')
        infile = open(sites[site]['SM_FILE'], 'rb')
        reader = csv.reader(infile)
        writer = csv.writer(sitefile)
        # fileinfo = next(reader, None)  # skip the first line
        fieldnames = next(reader, None)  # These are the headers
        # units = next(reader, None)  # Units of observation
        # sample = next(reader, None)  # Method of data sampling
        reader_dict = csv.DictReader(infile, fieldnames=fieldnames)
        writer.writerow(make_header())
        depths = 'Depths: 005=0.05m/010=0.10m/020=0.20m/030=0.30m/100=1.00m'
        descp = ['Time Zone: UTC+3', depths]
        writer.writerow(descp)
        writer.writerow(
            [sites[site]['header'], time.strftime(
                "%a, %d %b %Y %H:%M GMT", time.gmtime())])

        for line in reader_dict:
            row = []

            TIMESTAMP = format(str(line['TIMESTAMP']))
            row.extend([TIMESTAMP])

            t_air = str('{0:.1f}').format(float(line['t_hmp_Avg']))
            if t_air is "NAN":
                t_air == str(-8888)
            prec = str('{0:.1f}').format(float(line['Rain_mm_Tot']))
            if prec is "NAN":
                prec == str(-8888)
            row.extend([t_air, prec])

            row.insert(0, SITE + sites[site]['num'])
            # Add soil moisture data
            if line['VW_05cm_Avg'] == 'NAN':
                SM005 = str(-8888)
            else:
                SM005 = str('{0:.3f}').format(float(line['VW_05cm_Avg']))

            if line['VW_20cm_Avg'] == 'NAN':
                SM020 = str(-8888)
            else:
                try:
                    SM020 = str('{0:.3f}').format(float(line['VW_20cm_Avg']))
                except:
                    SM020 = str(-8888)

            if line['Temp_05cm_Avg'] == 'NAN':
                TS005 = str(-8888)
            else:
                try:
                    TS005 = str('{0:.1f}').format(float(line['Temp_05cm_Avg']))
                except:
                    TS005 = str(-8888)

            if line['Temp_20cm_Avg'] == 'NAN':
                TS020 = str(-8888)
            else:
                try:
                    TS020 = str('{0:.1f}').format(float(line['Temp_20cm_Avg']))
                except:
                    TS020 = str(-8888)
            row.extend([SM005, TS005, SM020, TS020])
            writer.writerow(row)
    return files_for_upload
