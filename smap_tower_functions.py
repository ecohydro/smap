"""
Do Temperature corrections for soil moisture data.

This script belongs in KenyaLab/Data/Tower/TowerData/SMAP
and is used to create properly format SMAP files for
transfer. This includes a temperature correction.
The script takes smapdata as input and return
individual station files as output.

This version is written for the tower soil mositure data.
It creates a seperate file for each of the soil moisture sampling locations
at the tower.

See smap_sampling_locations.py) for code to handle
additional soil moisture installations that are distributed around the
basin.

Hilary Wayland
2015-06-29

- edits:
K. Caylor, March 2016:
    - PEP8 edits
    - Cleaned up code
    - removed imports for make_row and make_header
    - deleted redundant code and old crap.



"""
import csv
import time


def make_row(line):
    """Create the row."""
    row = []
    row.extend([
        line['Year'],
        line['Month'],
        line['DOM'],
        line['Hour'],
        line['Minute']
    ])
    time_of_year = (
        (float(line['Hour']) + float(line['Minute']) / 60) / 24 +
        float(line['Day_of_Year'])
    )
    row.append(str('{0:.2f}').format(time_of_year))
    return row


def make_header():
    """Create the header."""
    header = []
    header.extend([
        'ID', 'Yr', 'Mo', 'Day', 'Hr', 'Min', 'TOY',
        'Tair', 'Prec', 'SM005', 'TS005', 'SM010', 'TS010',
        'SM020', 'TS020', 'SM030', 'TS030', 'SM100', 'TS100'
    ])
    return header


def correct_period(period_uncorr, temp):
    """Correct period."""
    return float(period_uncorr) + (20 - float(temp)) *\
        (0.526 - 0.052 * float(period_uncorr) + 0.00136 *
            float(period_uncorr)**2)


def calc_vwc(period, temp=None):
    """Calculate VWC."""
    if temp > -8888:
        period = correct_period(period, temp)
    return -0.0663 - 0.0063 * float(period) + (0.0007 * (float(period)**2))


def make_var_name(var, depth, site):
    """Create variables names."""
    if var == 'PA':
        return var + depth + 'cm' + site + '_Avg'
    elif var == 'VW':
        return var + depth + 'cm' + site + '_v0_Avg'
    elif var == 'Tsoil':
        if depth == '005':
            return 'Tsoil' + site + '_Avg'
        elif depth == '010':
            return 'Tsoil' + '10cm' + site + '_Avg'
        elif depth == '020':
            return 'Tsoil' + '20cm' + site + '_Avg'
        else:
            return None


def make_smap_data_for_tower_sites():
    """Create the smap file from the tower site smapdata file."""
    # SITE INFORMATION
    SITE = '2401'   # SMAP site #
    SM_FILE = 'smapdata'

    """
    Station information:

    Tree:
    "Tsoil10cmTree_Avg","Tsoil20cmTree_Avg",
    "VW005cmTree_v0_Avg","VW010cmTree_v0_Avg","VW020cmTree_v0_Avg","VW030cmTree_v0_Avg","VW100cmTree_v0_Avg",

    Grass:
    "Tsoil10cmGrass_Avg","Tsoil20cmGass_Avg",
    "VW005cmGrass_v0_Avg","VW010cmGrass_v0_Avg","VW020cmGrass_v0_Avg","VW030cmGrass_v0_Avg","VW100cmGrass_v0_Avg"

    Open:
    "Tsoil10cmOpen_Avg","Tsoil20cmOpen_Avg",
    "VW005cmOpen_v0_Avg","VW010cmOpen_v0_Avg","VW020cmOpen_v0_Avg","VW030cmOpen_v0_Avg","VW100cmOpen_v0_Avg",

    Riparian:
    "TsoilRiparian10cm_Avg",
    "VW005cmRiparian_v0_Avg","VW010cmRiparian_v0_Avg","VW020cmRiparian_v0_Avg","VW030cmRiparian_v0_Avg","VW100cmRiparian_v0_Avg"

    """

    # Determine the year, month, and day based on last line of infile:
    with open(SM_FILE, 'rb') as infile:
        import shlex
        # Determine the year, month, and day based on last line of infile:
        last_line = infile.readlines()[-1:]
        # assume timestamp is the first column
        # use shlex to get rid of double quotes in time stamp string
        ts = shlex.split(last_line[0])[0].split()[0]

    TREE_FILE = SITE + '001' + '_' + ''.join(ts[:10].split('-')) + '.txt'
    GRASS_FILE = SITE + '002' + '_' + ''.join(ts[:10].split('-')) + '.txt'
    OPEN_FILE = SITE + '003' + '_' + ''.join(ts[:10].split('-')) + '.txt'
    RIP_FILE = SITE + '004' + '_' + ''.join(ts[:10].split('-')) + '.txt'

    infile = open(SM_FILE, 'rb')
    treefile = open(TREE_FILE, 'wb')
    grassfile = open(GRASS_FILE, 'wb')
    openfile = open(OPEN_FILE, 'wb')
    ripfile = open(RIP_FILE, 'wb')

    reader = csv.reader(infile)
    t_writer = csv.writer(treefile)
    g_writer = csv.writer(grassfile)
    o_writer = csv.writer(openfile)
    r_writer = csv.writer(ripfile)

    fieldnames = next(reader, None)  # These are the headers
    reader_dict = csv.DictReader(infile, fieldnames=fieldnames)

    # Write the header
    t_writer.writerow(
        ['Mpala Research Center, Soil Moisture Station 1 (Tree)',
            time.strftime("%a, %d %b %Y %H:%M GMT", time.gmtime())]
    )
    g_writer.writerow(
        ['Mpala Research Center, Soil Moisture Station 2 (Grass)',
            time.strftime("%a, %d %b %Y %H:%M GMT", time.gmtime())]
    )
    o_writer.writerow(
        ['Mpala Research Center, Soil Moisture Station 3 (Open)',
            time.strftime("%a, %d %b %Y %H:%M GMT", time.gmtime())]
    )
    r_writer.writerow(
        ['Mpala Research Center, Soil Moisture Station 4 (Riparian)',
            time.strftime("%a, %d %b %Y %H:%M GMT", time.gmtime())]
    )

    # All SMAP files have the same SMAP header:
    header = make_header()
    t_writer.writerow(header)
    g_writer.writerow(header)
    o_writer.writerow(header)
    r_writer.writerow(header)

    depths = 'Depths: 005=0.05m/010=0.10m/020=0.20m/030=0.30m/100=1.00m'
    descp = ['Time Zone: UTC+3', depths]
    t_writer.writerow(descp)
    g_writer.writerow(descp)
    o_writer.writerow(descp)
    r_writer.writerow(descp)

    obs_depths = ['005', '010', '020', '030', '100']

    sites = {
        'Tree': {
            'num': '001',
            'writer': t_writer
        },
        'Grass': {
            'num': '002',
            'writer': g_writer
        },
        'Open': {
            'num': '003',
            'writer': o_writer
        },
        'Riparian': {
            'num': '004',
            'writer': r_writer
        }
    }

    try:
        # Iterate through all the lines in the input file.
        for line in reader_dict:
            # line = next(reader_dict)
            row = []

            TIMESTAMP = format(str(line['TIMESTAMP']))
            row.extend([TIMESTAMP])

            t_air = line.get('t_hmp_Avg') or -8888
            if "NAN" in t_air.upper():
                t_air = -8888
            rain = line.get('rainfall_Tot') or -8888
            if "NAN" in rain.upper():
                rain = -8888
            t_air = str('{0:.1f}').format(float(t_air))
            prec = str('{0:.1f}').format(float(rain))
            row.extend([t_air, prec])

            for site in sites.keys():
                # Create a temporary site row for this site
                site_row = list(row)
                # Add the site number to this site row
                site_row.insert(0, SITE + sites[site]['num'])
                # iterate through all the depths for this site
                # NOTE: we assume all sites have the same depths
                for obs_depth in obs_depths:
                    vwc = make_var_name('VW', obs_depth, site)
                    pa = make_var_name('PA', obs_depth, site)
                    tsoil = make_var_name('Tsoil', obs_depth, site)
                    soil_moisture = None
                    soil_temperature = None

                    if tsoil:  # Get the current value of temp if we have it:
                        soil_temperature = float(line.get(tsoil) or -8888)
                    # Check to see if we have period data for this depth:
                    if line[pa] == 'NAN':  # If not, then use a -8888
                        soil_moisture = -8888
                    else:  # If we do, then caluclate the corrected VWC:
                        soil_moisture = calc_vwc(
                            float(line[pa]),
                            soil_temperature)
                    # If we don't have a temp value for this depth, use -8888:
                    if soil_temperature is None:
                        soil_temperature = -8888
                    site_row.extend([
                        str('{0:.1f}').format(soil_moisture),
                        str('{0:.1f}').format(soil_temperature)])
                # Now that we are done, we should have everything we need:
                sites[site]['writer'].writerow(site_row)
    finally:
        infile.close()
        treefile.close()
        grassfile.close()
        openfile.close()
        ripfile.close()
    return [TREE_FILE, GRASS_FILE, OPEN_FILE, RIP_FILE]
