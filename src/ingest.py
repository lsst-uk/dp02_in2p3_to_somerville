import os
import subprocess

# Directory path
directory = '~/dp02_in2p3_to_somerville/ecsv'


# Iterate over each file in the directory
for filename in os.listdir(directory):
    run = filename.split('_')[-1].split['.'][0]
    print(run)
    # Check if the path is a file (not a directory)
    filepath = os.path.join(directory, filename)
    if os.path.isfile(filepath):
        # Construct the Unix command with the filepath
        command_to_run = f"butler ingest-files . deepCoadd_calexp 2.2i/runs/DP0.2/{run} {filepath}"
        
        # Execute the Unix command using subprocess
        subprocess.run(command_to_run, shell=True)
