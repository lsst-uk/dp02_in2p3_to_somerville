import getopt
import os
import sys

from astropy.table import Table
import pandas as pd
from collections import defaultdict
#-----------------------------------------------------------------------------

base_dir = '/data/butler/dp02/'
groups = {"deepCoadd": ['filename', 'band', 'skymap', 'tract', 'patch'],
          "object": ['filename', 'skymap', 'tract', 'patch']}
verbose = 0

# set panda options for table views
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.options.display.max_colwidth = None
pd.options.display.expand_frame_repr = False

#-----------------------------------------------------------------------------

def change_base_directory(url):
    directories=url.split('/')[3:]
    dtype = directories[7]
    
    if "deepCoadd" in dtype:
        band = directories[-2]
        patch = int(directories[-3])
        tract = int(directories[-4])
        run = directories[-6]
    elif "object" in dtype:
        band = 'None'
        patch = int(directories[-2])
        tract = int(directories[-3])
        run = directories[-5]
    else:
        print("[ERROR] dataType f{dtype} not supported.")
        raise SystemExit

    skymap = 'DC2'

    new_url = os.path.join(base_dir, *directories)
    if verbose > 10:
        print("URL:",url)
        print("DIR:",len(directories),directories)
        print("NURL:",new_url)
        print("bstpr:",band, skymap, tract, patch, run)
        raise SystemExit
    return new_url, band, skymap, tract, patch, run

#-----------------------------------------------------------------------------

def processData(dataType, splitobs=False, testRun=False):
    # read dataType files into 'urls'
    csvPath = os.path.join('../csv_files', f'{dataType}_urls.csv')
    urls = pd.read_csv(csvPath, header=None, names=['urls'])

    # change the html link into the base dir
    urls['filename'], urls['band'], urls['skymap'], urls['tract'], urls['patch'], urls['run']= zip(
        *urls['urls'].apply(lambda x: change_base_directory(x)))

    # deep copy the existing data to manipulate and delete duplicates
    urlstoo = urls.copy(deep=True)

    # create a duplicated column
    urlstoo['dupl'] = urlstoo.duplicated(subset=['band','tract','patch'],
                                         keep='last')

    # sort the duplicates
    #urlstoo.sort_values(by=['band','tract','patch','run'],axis=0,inplace=True)

    if verbose > 2:
        # check the 'urlstoo' object
        print(urlstoo.head())
        print(urlstoo[['run','band','tract','patch','dupl','filename']].loc[urlstoo['dupl'] == True])
        print(urls['filename'],urls['band'],urls['tract'],urls['patch'])
        print(urls['run'].value_counts())

    # drop the first duplicates and keep the last
    print("w/dupl:", urlstoo.shape)
    urlstoo.drop_duplicates(subset=['band','tract','patch'], keep='last',
                            inplace=True, ignore_index=True)
    print("uniqued:", urlstoo.shape)

    # copy the urls list to be used in the file creation
    #useUrls = urls.copy(deep=True)
    useUrls = urlstoo.copy(deep=True)

    if "object" in dataType:
        outGroups = groups["object"]
    elif "deepCoadd" in dataType:
        outGroups = groups["deepCoadd"]
    else:
        print("[ERROR] dataType doesn't exist in groups.")
        raise SystemExit

    # create files split by observation run
    if splitobs:
        by_run = useUrls.groupby('run')
        for run_value, group_df in by_run:
            new_group = group_df[outGroups]
            file_table = Table.from_pandas(new_group)
            if verbose > 2:
                print(file_table)

            # set out put path for ecsv file
            ecsvPath = os.path.join('./ecsv', f'{dataType}_{run_value}.ecsv')

            if testRun:
                print("[TEST] write to obsrun tables: f{ecsvPath}")
            else:
                print("Writing to obsrun table: f{ecsvPath}")
                ##file_table.write(f'./ecsv/deepCoadds0_{run_value}.ecsv')
                file_table.write(ecsvPath)

    # create one file containing all data
    new_urls = useUrls[outGroups]
    file_table = Table.from_pandas(new_urls)
    if verbose > 2:
        print(file_table)

    # set out put path for ecsv file
    ecsvPath = os.path.join('./ecsv', f'{dataType}.ecsv')

    if testRun:
        print("[TEST] write to single table: f{ecsvPath}")
    else:
        print("Writing to single table: f{ecsvPath}")
        file_table.write(ecsvPath)

        
#-----------------------------------------------------------------------------

def usage():
        print("Usage: python parse_to_ecsv.py [-s/--splitobs] [-h/--help] [-t/--test] datasetType")
        print("-s/--splitobs: create daily observation lists")
        print("-h/--help: print the help")
        print("-t/--test: test run, don't write data to files")
        print("datasetType: the dataset to be processed")

#------------------------------------------------------------------------------

def main(argv):
    dataType = ''
    splitObs = False
    testRun = False
    
    try:
        opts, args = getopt.getopt(argv[1:], "hst",
                                   ["help", "splitobs", "test"])

    except getopt.GetoptError:
        # print help information and exit:
        print(argv)
        usage()
        raise SystemExit

    for o, a in opts:
        if o in ("-s", "--splitobs"):
            splitObs = True
        if o in ("-h","--help"):
            usage()
            raise SystemExit
        if o in ("-t", "--test"):
            testRun = True

    if len(args) == 1:
        dataType = args[0]
    else:
        usage()
        raise SystemExit

    try:
        processData(dataType, splitObs)

    except Exception as e:
        print("An exception occurred:", str(e))
        with open(f'parse_to_ecsv_{dataType}_error.log', 'w') as f:
            with redirect_stdout(f):
                traceback.print_exc()

#------------------------------------------------------------------------------

if __name__ == "__main__":
    main(sys.argv)
