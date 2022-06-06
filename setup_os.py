from opensearchpy import OpenSearch
import json
import argparse
from adsputils import setup_logging, load_config
import os

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
                    help="Deletes existing ads_scan_explorer index before creating a new fresh instance")
    args = parser.parse_args()


opensearch = OpenSearch(config.get("OPEN_SEARCH_URL", ""))
if args.delete:
    opensearch.indices.delete(index = config.get("OPEN_SEARCH_INDEX", ""))

os_mapping_file = "./docker/os/mappings.json"

with open(os_mapping_file, 'r') as f:
    index_dict = json.load(f)
    opensearch.indices.create(index = config.get("OPEN_SEARCH_INDEX", ""), body = index_dict)
opensearch.transport.close()