
import os
import uuid
from ADSScanExplorerPipeline.models import JournalVolume, VolumeStatus
from ADSScanExplorerPipeline.ingestor import parse_top_file, parse_dat_file, parse_image_files, identify_journals, upload_image_files, check_all_image_files_exists
from kombu import Queue
import ADSScanExplorerPipeline.app as app_module
# import adsmsg

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSScanExplorerPipeline('ads-scan-pipeline', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('process-volume', app.exchange, routing_key='process-volume'),
    Queue('investigate-new-volumes', app.exchange, routing_key='investigate-new-volumes'),
    Queue('rerun_error_volumes', app.exchange, routing_key='rerun_error_volumes'),
)

# ============================= TASKS ============================================= #

@app.task(queue='process-volume')
def task_process_volume(base_path: str, journal_volume_id: str, upload_files: bool = False):
    """
    Processes a journal volume
    """
    logger.info("Processing journal_volume id: %s", journal_volume_id)
    with app.session_scope() as session:
        vol = None
        try:
            vol = JournalVolume.get_from_id_or_name(journal_volume_id, session)
            vol.status = VolumeStatus.Processing
            session.add(vol)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Failed to get journal_volume: %s from db: %s", journal_volume_id, e)
            return

        try:
            top_filename = vol.journal + vol.volume + ".top"
            top_file_path = os.path.join(base_path, "lists", vol.type, vol.journal, top_filename)
            dat_file_path = top_file_path.replace(".top", ".dat")
            image_path = os.path.join(base_path, "bitmaps", vol.type, vol.journal, vol.volume, "600")
            for page in parse_top_file(top_file_path, vol, session):
                session.add(page)

            for article in parse_dat_file(dat_file_path, vol, session):
                session.add(article)

            check_all_image_files_exists(image_path, vol, session)

            for page in parse_image_files(image_path, vol, session):
                session.add(page)
            if upload_files:
                upload_image_files(image_path, vol, session)
                #TODO possibly upload OCR files as well
            vol.status = VolumeStatus.Done
        except Exception as e:
            session.rollback()
            logger.error("Failed to process journal_volume_id: %s due to: %s", str(journal_volume_id), e)
            try:
                journal_volume = JournalVolume.get_from_id_or_name(journal_volume_id, session)
                journal_volume.status = VolumeStatus.Error
                session.add(journal_volume)
                session.commit()
            except Exception as e2:
                logger.error("Failed setting error on volume: %s due to: %s", str(journal_volume_id), e2)
        else:
            session.commit()
    return session

@app.task(queue='investigate-new-volumes')
def task_investigate_new_volumes(base_path: str, upload_files: bool = False, process = True):
    """
    Investigate if any new or updated volumes exists
    """
    logger.info("Investigating new or changed volumes in %s", base_path)
    for vol in identify_journals(base_path):
        vol_to_process  = None
        with app.session_scope() as session:
            existing_vol = JournalVolume.get_from_obj(vol, session)
            if existing_vol:
                if vol.file_hash != existing_vol.file_hash:
                        existing_vol.status = VolumeStatus.Update
                        existing_vol.file_hash = vol.file_hash
                        session.add(existing_vol)
                        session.commit()
                        vol_to_process = existing_vol.id
            else:
                vol.status = VolumeStatus.New
                session.add(vol)
                session.commit()
                vol_to_process = vol.id
        if vol_to_process and process:
            task_process_volume.delay(base_path, vol_to_process, upload_files)
    return session

@app.task(queue='rerun_error_volumes')
def task_rerun_error_volumes(base_path: str, upload_files: bool = False):
    """
    Rerun all journal volumes with status 'Error'
    """
    volumes_to_process = []
    with app.session_scope() as session:
        for vol in JournalVolume.get_errors(session):
            volumes_to_process.append(vol.id)
    for vol_id in volumes_to_process:
        task_process_volume.delay(base_path, vol_id, upload_files)

if __name__ == '__main__':
    app.start()
