#!/usr/bin/env python
import os
import argparse
from distutils.util import strtobool
from ADSScanExplorerPipeline.tasks import task_process_new_volumes, task_process_volume, task_investigate_new_volumes

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
    parser.add_argument("--process-db",
                    dest="process_db",
                    required=False,
                    default="True",
                    type=str,
                    help="If the all information from the input folder should be processed for each volume")
    parser.add_argument("--upload-files",
                    dest="upload",
                    required=False,
                    default="True",
                    type=str,
                    help="If image files should be uploaded to the s3 bucket")
    parser.add_argument("--index-ocr",
                    dest="ocr",
                    required=False,
                    default="True",
                    type=str,
                    help="If ocr files should be index on opensearch")
    parser.add_argument("--upload-db",
                    dest="upload_db",
                    required=False,
                    default="True",
                    type=str,
                    help="If database should be uploaded to remote db")
    parser.add_argument("--force-update",
                    dest="force",
                    required=False,
                    default="False",
                    type=str,
                    help="Force updates of the volumes")

    subparsers = parser.add_subparsers(help='commands', dest="action")
    new_parser = subparsers.add_parser('NEW', help='Loops through input folder and processes all new or updated volumes')
    update_parser = subparsers.add_parser('UPDATE', help='Reprocesses all updated volumes')
    run_parser = subparsers.add_parser('SINGLE', help='Process single volume')
    
    new_parser.add_argument("--process",
                dest="process",
                required=False,
                default="True",
                type=str,
                help="If detected volumes should be processed")

    new_parser.add_argument("--dry-run",
                dest="dry_run",
                required=False,
                default="False",
                type=str,
                help="If volume detection should just be dry_run")

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
        process_db = False 
        if bool(strtobool(args.process_db)):
            process_db = True
        upload = False 
        if bool(strtobool(args.upload)):
            upload = True
        ocr = False 
        if bool(strtobool(args.ocr)):
            ocr = True
        upload_db = False 
        if bool(strtobool(args.upload_db)):
            upload_db = True
        force = False 
        if bool(strtobool(args.force)):
            force = True
        
        if args.action == "NEW":
            process = False 
            if bool(strtobool(args.process)):
                process = True
            dry_run = False 
            if bool(strtobool(args.dry_run)):
                dry_run = True
            logger.info("Process all new volumes in: %s", input_folder)
            task_investigate_new_volumes.delay(input_folder, process_db, upload, ocr, upload_db, process, dry_run)
       
        elif args.action == "UPDATE":
            task_process_new_volumes.delay(input_folder, process_db, upload, ocr, upload_db, process_all=True, force_update=force)

        elif args.action == "SINGLE":
            for id in args.ids:
                logger.info("Process volume: %s in: %s", id, input_folder)
                task_process_volume.delay(input_folder, id, process_db, upload, ocr, upload_db, force_update=True)
