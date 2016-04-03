"""Process SMAP data and post it to NASA's ftp server.

K. Caylor, April 2, 2016

Steps:

1.
2.
3.
4.
5.
6.


"""
import os
from ftplib import FTP
from subprocess import call, check_output
from downscalers import downscale_to_hourly
from smap_tower_functions import make_smap_data_for_tower_sites
from smap_basin_functions import make_smap_data_for_basin_sites


def cleanup():
    """Remove temporary files."""
    commands = [
        'rm sixtyMinuteTable*',
        'rm tenMinuteTable',
        'rm smapdata*',
        'rm table1.dat',
        'rm table2.dat',
        'rm table3.dat',
        'rm table4.dat',
        'rm table5.dat',
        'rm upper.dat',
        'rm flux.dat',
    ]
    print "Cleaning up temporary files."
    [call(command, shell=True) for command in commands]


def upload(ftp, file):
    """Upload a file to an ftp site."""
    ext = os.path.splitext(file)[1]
    if ext in (".txt", ".htm", ".html"):
        # By default, we do not over-write.
        if not ftp.size(file):
            ftp.storlines("STOR " + file, open(file))
            print "Uploaded {file} to {loc}".format(
                file=file,
                loc=ftp.host + ftp.pwd()
            )
        else:
            print "{file} already exists at {loc}".format(
                file=file,
                loc=ftp.host + ftp.pwd())
    else:
        # By default, we do not over-write.
        if not ftp.size(file):
            ftp.storbinary("STOR " + file, open(file, "rb"), 1024)
            print "Uploaded {file} to {loc}".format(
                file=file,
                loc=ftp.host + ftp.pwd()
            )
        else:
            print "{file} already exists at {loc}".format(
                file=file,
                loc=ftp.host + ftp.pwd())


def ftp_files(file_list):
    """Transfer SMAP files to ftp server."""
    # SET UP ftp TRANSFER
    host = 'enso.princeton.edu'       # server address
    ftp = FTP(host)
    ftp.login()
    ftp.cwd('incoming/Caylor_smap/')
    for file in file_list:
        upload(ftp, file)
    return "SMAP transfer successful"


def join(left_file, right_file, output_file):
    """Join two files into a third."""
    delim = ','
    cmd = 'join -t "{delim}" '.format(delim=delim)
    cmd += '{left_file} {right_file} > {output_file}'.format(
        left_file=left_file,
        right_file=right_file,
        output_file=output_file)
    call(cmd, shell=True)


def cut(input_file, output_file, variables=[]):
    """Cut out columns from a datafile and save to new file.

    Assumes comma-delimited

    param: input_file - Datafile to remove columns from
    param: output_file - New file to be created from selected columns
    param: columns - List of columns to cut
    """
    if variables:
        file_vars = get_vars(input_file)
        columns = [file_vars.index(var) for var in variables]
        cols = ','.join([str(x+1) for x in columns])
        cut_cmd = 'cut -d ","  -f {cols} '.format(
            cols=cols
        )
        cut_cmd += '"{input_file}" > {output_file}'.format(
            input_file=input_file,
            output_file=output_file)
        call(cut_cmd, shell=True)


def remove_lines(input_file, remove_lines=[]):
    """Remove specified lines from a file.

    param: input_file - The name of the file to workon.
        This file will be over-written in place.

    param: remove_lines - An array of lines to remove.
    """
    lines = open(input_file, 'r').readlines()
    keepers = set(range(len(lines))) - set(remove_lines)
    outlines = [lines[i] for i in keepers]
    open(input_file, 'w').writelines(outlines)


def get_vars(input_file):
    """Return the list of variables from a datafile.

    Assumes data file is comma delimited.

    Returns a list of variables found in the file.

    """
    cmd = 'head -2 "{filename}" | tail -1'.format(filename=input_file)
    variables = check_output(cmd, shell=True)
    return [x.rstrip() for x in variables.split(',')]


def variables_to_keep(variables=[]):
    """Filter columns based on variable names."""
    vars_to_keep = []
    for i in range(len(variables)):
        name = variables[i]
        if name == '"RECORD"':
            continue
        if name[:5] == '"Batt':
            continue
        if name[-4:] == 'Std"':
            continue
        if name[-2:] == ')"':
            continue
        if name[:4] == '"shf':
            continue
        if name == '"uSecond"' or name == '"WeekDay"':
            continue
        if name[:4] == '"del':
            continue
        if name[:4] == '"bad' or name[:7] == '"broken' or name[:6] == '"moved':
            continue
        vars_to_keep.append(name)
    return vars_to_keep

# STEP 1: Define files and create temporary files with needed variables.
(data_dir, _) = os.path.split(os.getcwd())
upper_file = data_dir + '/' + 'CR5000_SN2446_upper.dat'
soil_moisture_file = data_dir + '/' + 'CR3000_SN9945_Table1.dat'
flux_data_file = data_dir + '/' + 'CR3000_SN4709_flux.dat'

# Remove the timestamp and last column from the upper file and save
# result to upper.dat
success = cut(
    upper_file,
    'upper.dat',
    variables=['"TIMESTAMP"', '"rainfall_Tot"'])
success = cut(
    soil_moisture_file,
    'table1.dat',
    variables=variables_to_keep(get_vars(soil_moisture_file))
)
success = cut(
    flux_data_file,
    'flux.dat',
    variables=['"TIMESTAMP"', '"t_hmp_Avg"'])
# Trim the CSI junk out of flux.dat (meta, units, type)
remove_lines('flux.dat', [0, 2, 3])
remove_lines('table1.dat', [0, 2, 3])
remove_lines('upper.dat', [0, 2, 3])

# STEP 2: Process the CR216 files.
# All the CR216 files. We should get CR216_SN22027_soil.dat someday.
CR216_files = {
    'CR216_SN22028_soil.dat': [
        'table2.dat',
        'sixtyMinuteTable2',
        'smapdata2'],
    'CR216_SN22029_soil.dat': [
        'table3.dat',
        'sixtyMinuteTable3',
        'smapdata3'],
    'CR216_SN22030_soil.dat': [
        'table4.dat',
        'sixtyMinuteTable4',
        'smapdata4'],
    'CR216_SN22031_soil.dat': [
        'table5.dat',
        'sixtyMinuteTable5',
        'smapdata5']
}

# Process each soil file, keeping only the good columns.
for soil_file in CR216_files.keys():
    this_file = data_dir + '/' + soil_file
    success = cut(
        this_file,
        CR216_files[soil_file][0],
        variables=variables_to_keep(get_vars(this_file))
    )

# STEP 3: Combine data before downscaling.
join('table1.dat', 'upper.dat', 'tenMinuteTable')

# STEP 4: Downscale everything to hourly data.
# Downscale the Tower site rainfall, soil moisture, and temp data.
# Note: Variable names are always wrapped in double quotes.
tenMinuteTable_sum_vars = ['"rainfall_Tot"']
tenMinuteTable_sample_vars = [
    '"VW005cmTree_v0_Avg"',
    '"VW010cmTree_v0_Avg"',
    '"VW020cmTree_v0_Avg"',
    '"VW030cmTree_v0_Avg"',
    '"VW100cmTree_v0_Avg"',
    '"VW005cmGrass_v0_Avg"',
    '"VW010cmGrass_v0_Avg"',
    '"VW020cmGrass_v0_Avg"',
    '"VW030cmGrass_v0_Avg"',
    '"VW100cmGrass_v0_Avg"',
    '"VW005cmRiparian_v0_Avg"',
    '"VW010cmRiparian_v0_Avg"',
    '"VW020cmRiparian_v0_Avg"',
    '"VW030cmRiparian_v0_Avg"',
    '"VW100cmRiparian_v0_Avg"',
    '"VW005cmOpen_v0_Avg"',
    '"VW010cmOpen_v0_Avg"',
    '"VW020cmOpen_v0_Avg"',
    '"VW030cmOpen_v0_Avg"',
    '"VW100cmOpen_v0_Avg"',
    '"PA005cmTree_Avg"',
    '"PA010cmTree_Avg"',
    '"PA020cmTree_Avg"',
    '"PA030cmTree_Avg"',
    '"PA100cmTree_Avg"',
    '"PA005cmGrass_Avg"',
    '"PA010cmGrass_Avg"',
    '"PA020cmGrass_Avg"',
    '"PA030cmGrass_Avg"',
    '"PA100cmGrass_Avg"',
    '"PA005cmRiparian_Avg"',
    '"PA010cmRiparian_Avg"',
    '"PA020cmRiparian_Avg"',
    '"PA030cmRiparian_Avg"',
    '"PA100cmRiparian_Avg"',
    '"PA005cmOpen_Avg"',
    '"PA010cmOpen_Avg"',
    '"PA020cmOpen_Avg"',
    '"PA030cmOpen_Avg"',
    '"PA100cmOpen_Avg"',
    '"Tsoil10cmTree_Avg"',
    '"Tsoil20cmTree_Avg"',
    '"Tsoil10cmGrass_Avg"',
    '"Tsoil20cmGrass_Avg"',
    '"Tsoil10cmOpen_Avg"',
    '"Tsoil20cmOpen_Avg"',
]

downscale_to_hourly(
    input_file='tenMinuteTable',
    output_file='sixtyMinuteTable',
    sum_vars=tenMinuteTable_sum_vars,
    sample_vars=tenMinuteTable_sample_vars)

cmd = 'join -t "," -a1 -1 1 -2 1 -e "-8888" {inf} flux.dat > {outf}'.format(
    inf='sixtyMinuteTable',
    outf='smapdata')
call(cmd, shell=True)

# Downscale the SMAP site rainfall, soil moisture, and temp data.
CR216_sum_vars = ['"Rain_mm_Tot"']
CR216_sample_vars = [
    '"VW_20cm_Avg"',
    '"VW_05cm_Avg"',
    '"Temp_20cm_Avg"',
    '"Temp_05cm_Avg"'
]

for soil_file in CR216_files.keys():
    downscale_to_hourly(
        input_file=CR216_files[soil_file][0],
        output_file=CR216_files[soil_file][1],
        sum_vars=CR216_sum_vars,
        sample_vars=CR216_sample_vars
    )
    # Do the major joins, with substitution:
    cmd = 'join -t "," -a2 -e "-8888" -o 0,2.2,2.3,2.4,2.5,2.6,1.2 '
    cmd += 'flux.dat {joinfile} > {outfile}'.format(
        joinfile=CR216_files[soil_file][1],
        outfile=CR216_files[soil_file][2]
    )
    call(cmd, shell=True)

# STEP 5: Make the final SMAP data files that will be FTP'd
files_for_upload = []
tower_files = make_smap_data_for_tower_sites()
basin_files = make_smap_data_for_basin_sites()
files_for_upload.extend(tower_files)
files_for_upload.extend(basin_files)

ftp_files(files_for_upload)
cleanup()

print "We made it."
