
import os
import uuid
from ADSScanExplorerPipeline.models import JournalVolume
from ADSScanExplorerPipeline.pages import read_top_file, read_dat_file, read_image_files
from kombu import Queue
from google.protobuf.json_format import MessageToDict
from datetime import datetime
import ADSScanExplorerPipeline.app as app_module
import adsmsg

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSScanExplorerPipeline('ads-scan-pipeline', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('process-citation-changes', app.exchange, routing_key='process-citation-changes'),
    Queue('maintenance_reevaluate', app.exchange, routing_key='maintenance_reevaluate'),
    Queue('output-results', app.exchange, routing_key='output-results'),
)

# ============================= TASKS ============================================= #

@app.task(queue='process-new-volume')
def task_process_new_volume(journal_volume_id: uuid, force=False):
    """
    Process new volume
    """
    with app.session_scope() as session:
        session.begin()
        try:
            file_path = "something"
            journal_volume = JournalVolume.get(journal_volume_id, session)
            read_top_file(file_path, journal_volume, session)
            read_dat_file(file_path, journal_volume, session)
            read_image_files(file_path, journal_volume, session)
            #TODO copy image files
        except:
            session.rollback()
            raise
        else:
            session.commit()

    

if __name__ == '__main__':
    app.start()
