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
    parser.add_argument("--update-settings",
                        dest="update",
                        action='store_true',
                        required=False,
                        default=False,
                        help="Update synonym and stopword tokens")
    args = parser.parse_args()


opensearch = OpenSearch(config.get("OPEN_SEARCH_URL", ""))
if args.delete:
    opensearch.indices.delete(index=config.get("OPEN_SEARCH_INDEX", ""))

os_config_file = "./docker/os/config.json"
os_syn_file = "./docker/os/data/ads_text.syn"
os_stop_file = "./docker/os/data/ads_text.stop"

with open(os_config_file, 'r') as config_file:
    index_dict = json.load(config_file)
    config_file.close()

with open(os_syn_file) as syn_file:
    synonyms = []
    for line in syn_file:
        if not line.strip().startswith('#'):
            synonyms.append(line)
    syn_file.close()

with open(os_stop_file) as stop_file:
    sw = stop_file.readlines()
    stopwords = [s.strip() for s in sw]
    stop_file.close()

index_dict['settings']['index']['analysis']['filter']['syn_filter']['synonyms'] = synonyms
index_dict['settings']['index']['analysis']['filter']['stop_filter']['stopwords'] = stopwords

index = config.get("OPEN_SEARCH_INDEX", "")
if args.update:
    opensearch.indices.close(index=index)
    opensearch.indices.put_settings(index=index, body=index_dict['settings'])
    opensearch.indices.open(index=index)
else:
    opensearch.indices.create(index=index, body=index_dict)

opensearch.transport.close()
