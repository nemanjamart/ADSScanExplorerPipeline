
import os
import uuid
from ADSScanExplorerPipeline.ADSScanExplorerPipeline.models import VolumeStatus
from ADSScanExplorerPipeline.models import JournalVolume
from ADSScanExplorerPipeline.ingestor import parse_top_file, parse_dat_file, parse_image_files, identify_journals
from kombu import Queue
from datetime import datetime
import ADSScanExplorerPipeline.app as app_module
import adsmsg

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSScanExplorerPipeline('ads-scan-pipeline', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('process-volume', app.exchange, routing_key='process-volume'),
    Queue('investigate-new-volumes', app.exchange, routing_key='investigate-new-volumes'),
)

# ============================= TASKS ============================================= #

@app.task(queue='process-volume')
def task_process_volume(journal_volume_id: uuid):
    """
    Processes a journal volume
    """
    with app.session_scope() as session:
        session.begin()
        try:
            file_path = "something"
            journal_volume = JournalVolume.get(journal_volume_id, session)
            for page in parse_top_file(file_path, journal_volume):
                session.add(page)

            for article in parse_dat_file(file_path, journal_volume):
                session.add(article)

            for page in parse_image_files(file_path, journal_volume):
                session.add(page)
                
            #TODO copy image files
        except:
            session.rollback()
            journal_volume.status = VolumeStatus.Error
            session.add(journal_volume)
            session.commit()
            raise
        else:
            session.commit()

@app.task(queue='investigate-new-volumes')
def task_investigate_new_volumes():
    """
    Investigate if any new or updated volumes exists
    """
    with app.session_scope() as session:
        #TODO
        base_path = ""
        for vol in identify_journals():
            existing_vol = JournalVolume.get_from_obj(vol, session)
            if existing_vol:
                if vol.file_hash != existing_vol.file_hash:
                    existing_vol.status = VolumeStatus.Update
                    existing_vol.file_hase = vol.file_hash
                    session.add(existing_vol)
                    session.commit()
                    task_process_volume.delay(existing_vol.id)
            else:
                vol.status = VolumeStatus.New
                session.add(vol)
                session.commit()
                task_process_volume.delay(vol.id)

if __name__ == '__main__':
    app.start()
