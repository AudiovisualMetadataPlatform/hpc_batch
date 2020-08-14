#!/bin/env python3
import argparse
from pathlib import Path
import logging
import tempfile
import yaml
from time import sleep
import sys

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger()

def main():
    """
    Submit a job to run ina speech segmenter on HPC
    """
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument("--quiet", default=False, action="store_true", help="Turn off output")
    parser.add_argument("dropbox", help="hpc_batch dropbox location")
    parser.add_argument("input", help="input audio file")
    parser.add_argument("segments", help="INA Speech Segmenter output")

    args = parser.parse_args()

    # set up logging
    if args.debug:
        logger.setLevel(logging.DEBUG)  
    else:
        logger.setLevel(logging.INFO)

    if args.quiet:
        logger.setLevel(logging.ERROR)

    dropbox = Path(args.dropbox)
    if not dropbox.exists() or not dropbox.is_dir():
        logger.error("Dropbox doesn't exist or isn't a directory")
        exit(1)

    # job parameters    
    job = {
        'script': 'inaspeech',
        'input_map': {
            'input': args.input
        },
        'output_map': {
            'segments': args.segments
        }
    }

    # submit the job
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".job",
                                     dir=dropbox, delete=False) as f:
        result_file = Path(f.name + ".finished")
        logger.debug(f"Job file: {f.name}, result file: {result_file}")
        yaml.dump(job, f)

    # wait for the job to complete
    while not result_file.exists():
        sleep(10)

    # read the results and respond accordingly
    with open(result_file) as f:
        job = yaml.load(f, Loader=yaml.SafeLoader)

    result_file.unlink()

    logger.info(f"Job status: {job['job']['status']}, message: {job['job']['message']}")
    logger.debug(f"STDERR: {job['job']['stderr']}")
    logger.debug(f"STDOUT: {job['job']['stdout']}")
    logger.debug(f"Return Code: {job['job']['rc']}")
    
    exit(0 if job['job']['status'] == 'ok' else 1)

if __name__ == "__main__":
    main()
