#!/usr/bin/env python
import os
from ADSScanExplorerPipeline.tasks import task_investigate_new_volumes

# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

# =============================== FUNCTIONS ======================================= #

if __name__ == '__main__':
    task_investigate_new_volumes.delay()
