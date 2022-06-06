#LOGGING_LEVEL = 'WARN'
#LOGGING_LEVEL = 'DEBUG'
LOGGING_LEVEL = 'INFO'
LOG_STDOUT = True

CELERY_INCLUDE = ['ADSScanExplorerPipeline.tasks']
CELERY_BROKER = 'pyamqp://user:password@localhost:5672/scan_explorer_pipeline'
OUTPUT_CELERY_BROKER = 'pyamqp://user:password@localhost:5672/master_pipeline'
OUTPUT_TASKNAME = 'adsmp.tasks.task_update_record'

S3_BUCKET = 'scan-explorer'
S3_BUCKET_ACCESS_KEY = 'CHANGE_ME'
S3_BUCKET_SECRET_KEY = 'CHANGE_ME'
OPEN_SEARCH_URL = 'http://opensearch-node1:9200'
OPEN_SEARCH_INDEX = 'scan-explorer'
SQLALCHEMY_URL = 'postgres://scan_explorer:scan_explorer@localhost:5432/scan_explorer_pipeline'
SQLALCHEMY_ECHO = False
# When 'True', no events are emitted to the broker via the webhook
TESTING_MODE = True
# When 'True', it converts all the asynchronous calls into synchronous,
# thus no need for rabbitmq, it does not forward to master
# and it allows debuggers to run if needed:
CELERY_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

# Sub-directories of the input folder pointing to the
# publication type directory containing book, seri, conf etc
TOP_SUB_DIR = 'lists'
BITMAP_SUB_DIR='bitmaps'
OCR_SUB_DIR='ocr/full'