#!/usr/bin/env python
import os
import argparse
from distutils.util import strtobool
from ADSScanExplorerPipeline.tasks import task_investigate_new_volumes, task_rerun_error_volumes, task_process_volume

# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

# =============================== FUNCTIONS ======================================= #

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-folder",
                    dest="input_folder",
                    required=True,
                    type=str,
                    help="Path to the base folder of all lists and bitmaps of all journals")
    parser.add_argument("--upload-files",
                    dest="upload",
                    required=False,
                    default="False",
                    type=str,
                    help="If image files should be uploaded to the s3 bucket")
    parser.add_argument("--index-ocr",
                    dest="ocr",
                    required=False,
                    default="False",
                    type=str,
                    help="If ocr files should be index on elasticsearch")
    subparsers = parser.add_subparsers(help='commands', dest="action")
    subparsers.add_parser('NEW', help='Loops through input folder and processes all new or updated volumes')
    subparsers.add_parser('ERROR', help='Process all volumes which have encountered errors in previous ingestions')
    run_parser = subparsers.add_parser('SINGLE', help='Process single volume')
    run_parser.add_argument('--id',
                        dest='ids',
                        nargs='+',
                        required=True,
                        type=str,
                        help='Space separated ids, either uuid found in DB or journal(5 chars)_volume(4 chars) e.g. ApJ..0333')
    # maintenance_parser = subparsers.add_parser('MAINTENANCE', help='Execute maintenance task')

    args = parser.parse_args()
    input_folder = os.path.join(proj_home, args.input_folder)
    if not os.path.exists(args.input_folder):
        parser.error("the folder '{}' does not exist".format(input_folder))
    elif not os.access(args.input_folder, os.R_OK):
        parser.error("the folder '{}' cannot be accessed".format(input_folder))
    else:
        #TODO change to True by default before operational release
        upload = False 
        if bool(strtobool(args.upload)):
            upload = True
        ocr = False 
        if bool(strtobool(args.ocr)):
            ocr = True
        
        if args.action == "NEW":
            logger.info("Process all new volumes in: %s", input_folder)
            task_investigate_new_volumes.delay(input_folder, upload, ocr)
        elif args.action == "ERROR":
            logger.info("Process all volumes with previous errors in: %s", input_folder)
            task_rerun_error_volumes.delay(input_folder, upload)
        elif args.action == "SINGLE":
            for id in args.ids:
                logger.info("Process volume: %s in: %s", id, input_folder)
                task_process_volume.delay(input_folder, id, upload, ocr)
