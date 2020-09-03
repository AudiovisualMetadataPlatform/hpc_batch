#!/bin/env python3
import argparse
from pathlib import Path
import logging
import sys
import hpc_submit

def main():
    """
    Submit a job to run ina speech segmenter on HPC
    """
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument("dropbox", help="hpc_batch dropbox location")
    parser.add_argument("input", help="input audio file")
    parser.add_argument("kaldi_transcript_json", help="Kaldi JSON output")
    parser.add_argument("kaldi_transcript_txt", help="Kalid TXT output")
    parser.add_argument("amp_transcript_json", help="AMP JSON output")
    args = parser.parse_args()

    # set up logging
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(message)s")

    # job parameters    
    job = {
        'script': 'kaldi',
        'input_map': {
            'input': args.input
        },
        'output_map': {
            'kaldi_json': args.kaldi_transcript_json,
            'kaldi_txt': args.kaldi_transcript_txt,
            'amp_json': args.amp_transcript_json
        }
    }

    job = hpc_submit.submit_and_wait(args.dropbox, job)
    exit(0 if job['job']['status'] == 'ok' else 1)

if __name__ == "__main__":
    main()