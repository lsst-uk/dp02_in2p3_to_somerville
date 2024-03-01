import sys
import os
import subprocess
import traceback
import cProfile
from contextlib import redirect_stdout


# Iterate over each file in the directory
def do_ingestion(repo, ds_type, directory):

    for filename in os.listdir(directory):
        run = filename.split('_')[-1].split('.')[0]
        print(run)
        # Check if the path is a file (not a directory)
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            # Construct the butler command with the filepath
            command_to_run = f"butler ingest-files {repo} {ds_type} 2.2i/runs/DP0.2/{run} {filepath}"
            
            # Execute the butler command using subprocess
            subprocess.run(command_to_run, shell=True)


if __name__ == "__main__":
    
    len_argv = len(sys.argv)

    if len_argv < 3:
        print("Usage: python ingest.py repo datasetType profiler_output.prof")
        sys.exit(1)

    repo = sys.argv[1]
    ds_type = sys.argv[2]
    profiler_file = f'ingest_{ds_type}_output.prof' if len == 3 else sys.argv[3]

    directory = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)),
        '/ecsv/',
        ds_type)

    try:
        profiler = cProfile.Profile()
        profiler.enable()

        do_ingestion(repo,ds_type, directory)

        profiler.disable()
        profiler.dump_stats(profiler_file)

    except Exception as e:
        print("An exception occurred:", str(e))
        with open(f'ingest_{ds_type}_error.log', 'w') as f:
            with redirect_stdout(f):
                traceback.print_exc()
        profiler.disable()
        profiler.dump_stats(profiler_file)