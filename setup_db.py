from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import ADSScanExplorerPipeline.models
import argparse
import os
from adsputils import setup_logging, load_config

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('setup_db.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

# =============================== FUNCTIONS ======================================= #

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--re-create",
                    dest="delete",
                    action='store_true',
                    required=False,
                    default=False,
                    help="Deletes all existing ads_scan_explorer tables in the DB before creating fresh tables")
    args = parser.parse_args()
    engine = create_engine(config.get("SQLALCHEMY_URL", ""), echo=False)
    conn = engine.connect()
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    if args.delete:
        ADSScanExplorerPipeline.models.Base.metadata.drop_all(engine)
    ADSScanExplorerPipeline.models.Base.metadata.create_all(engine)
    session.commit()