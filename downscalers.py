"""Function for downscaling data."""
from datetime import datetime


def downscale_to_hourly(
        input_file=None,
        output_file=None,
        sum_vars=[],
        sample_vars=[]):
    """Downscale ten minute data to hourly.

    This script belongs in KenyaLab/Data/Tower/TowerData/SMAP and is used to
    resample 10 minute data to hourly records and sum hourly rainfall.


    Hilary Wayland
    2015-06-29

    K. Caylor
    2015-07-01

    Changes:

    2015-07-01 - complete re-write. Now hoovering up the whole file and then
    spitting out the data from a bunch of dicts. This could be a disaster
    if the file is sufficiently large, but I am not scared.

    2016-04-02 - handles any datafile from the tower using sum_columns
    and sample_columns.

    For CS200X.Std.03 files use these arguments:

    sum_cols = [5]
    sample_cols = [1, 3, 6, 7]

    For the tenMinuteTable, use these arguments:

    sum_cols = [59]
    sample_cols = [14, 16, 19, 21, ]
    """
    # This is the universal CSI date string format. It is always be the same.
    csi_date_format = '"%Y-%m-%d %H:%M:%S"'

    # Column locations of rainfall and soil moisture values.
    # These will need to be changed or modified for different datafiles.
    # They are correct for CR200X.Std.03.
    #
    # NOTE: Columns are zero indexed, so the TS column, which is the 1st column
    # in all CSI datafiles is column 0.
    #
    # sum_cols = [5]
    # sample_cols = [1, 3, 6, 7]

    # rain_column = 5  # Rain is rainfall (in mm) over the past 10 mins.
    # vwc_20cm_column = 1  # VWC is volumetric water content (soil moisture)
    # vwc_05cm_column = 3  # 20CM and 05CM refer to probe depths (from surface)
    # temp_20cm_column = 6
    # temp_05cm_column = 7

    # To downscalse to hourly, we need to do two things:
    # (1) For rainfall, we need to sum the total rain over the past hour
    # (2) For soil moisture, we need to return the soil moisture we observe
    # at the top of the hour.
    #
    # To do (1), we will first read the entire file, and create a dictionary of
    # data containing rainfall in each {year}{doy}{hour}. We will then iterate
    # through that dictionary and sum rainfall by the hour. We will flag any
    # hours that do not contain 6 observations.
    #
    # To do (2), we will simply sample the top of each hour's soil moisture.

    # The start of every CSI datafile contains four header rows. Read them now:
    in_file = open(input_file, 'r')
    out_file = open(output_file, 'w')
    # The first line contains information about the datafile:
    in_file.readline().rstrip()

    # The second line contains information about the variable names
    var_names = in_file.readline().rstrip().split(",")

    # The third line includes information on the variable units
    var_units = in_file.readline().rstrip().split(",")

    # The fourth line describes how the variable is recorded in the datalogger.
    # (e.g. is it an average, or a sample, etc...?)
    in_file.readline().rstrip().split(",")

    # Initialize our dictionaries:
    sum_dict = {}
    sample_dict = {}
    count_dict = {}
    for var in sum_vars:
        sum_dict[var] = {}
        count_dict[var] = {}
    for var in sample_vars:
        sample_dict[var] = {}

    # list of hours:
    all_hours = []
    # Now that we are at the actual data, let's load the entire file into a
    # dictionary organized by date:
    for line in in_file.readlines():
        # Before doing anything else, make sure to replace NaNs with -7777
        this_line = line.rstrip().replace('"NAN"', "-7777").split(",")
        this_ts = line.split(",")[0]
        this_time = datetime.strptime(this_ts, csi_date_format)

        # Figure out what hour this is:
        this_hour = datetime(
            this_time.year,
            this_time.month,
            this_time.day,
            this_time.hour)

        # Convert the datetime into an ordinal string for use in a dict:
        this_hour = str(this_hour.strftime('%Y%m%d%H'))
        all_hours.append(this_hour)
        # We need to create keys for year, doy, hours when first seen:
        for sum_var in sum_dict.keys():
            if not sum_dict[sum_var].get(this_hour):
                sum_dict[sum_var][this_hour] = 0
            if not count_dict[sum_var].get(this_hour):
                count_dict[sum_var][this_hour] = 0
        for sample_var in sample_dict.keys():
            if not sample_dict[sample_var].get(this_hour):
                sample_dict[sample_var][this_hour] = '-7777'

        # Okay, now let's add any new rainfall:
        for var in sum_vars:
            column = var_names.index(var)
            sum_dict[var][this_hour] += float(this_line[column])
            count_dict[var][this_hour] += 1

        if this_time.minute == 0:
            for var in sample_vars:
                column = var_names.index(var)
                sample_dict[var][this_hour] = float(
                    this_line[column])

    # Now we've read the whole file and we have a dictionary.
    # But it's not sorted, because that's not how Python roles.
    # So let's sort these bad boys.
    # There's no chance that we can have rainfall data w/o soil moisture data
    # Because they occur in the same lines.
    # So just get all the hours:
    hours = sorted(set(all_hours))

    this_var_list = ['"TIMESTAMP"']
    for var in sum_vars:
        col = var_names.index(var)
        this_var_list.append(var_names[col].rstrip())
    for var in sample_vars:
        col = var_names.index(var)
        this_var_list.append(var_names[col].rstrip())
    print >>out_file, ','.join(this_var_list)

    # Now iterate through the list and cough up the data:
    for hour in hours:
        # hour is a string (that's what dicts use).
        # Reverse the mojo we used to make the dictionary:
        this_date = datetime.strptime(hour, '%Y%m%d%H')
        # NOTE: this_date will need to be formatted somehow to match
        # whatever date string format you want to have.
        # NOTE: This is probably not the correct output format.
        # So... lots to do here, but the general plan is set.

        this_data = ['"{date}"'.format(date=this_date)]
        for var in sum_vars:
            this_data.append(str(sum_dict[var][hour]))
        for var in sample_vars:
            this_data.append(str(sample_dict[var][hour]))
        print>>out_file, ','.join(this_data)
