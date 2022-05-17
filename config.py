#LOGGING_LEVEL = 'WARN'
#LOGGING_LEVEL = 'DEBUG'
LOGGING_LEVEL = 'INFO'
LOG_STDOUT = True

CELERY_INCLUDE = ['ADSScanExplorerPipeline.tasks']
CELERY_BROKER = 'pyamqp://user:password@localhost:5672/scan_explorer_pipeline'
OUTPUT_CELERY_BROKER = 'pyamqp://user:password@localhost:5672/master_pipeline'
OUTPUT_TASKNAME = 'adsmp.tasks.task_update_record'

S3_BUCKET = 'scan-explorer'
ELASTIC_SEARCH_URL = 'http://localhost:9200'
ELASTIC_SEARCH_INDEX = 'scan-explorer'
SQLALCHEMY_URL = 'postgres://scan_explorer:scan_explorer@localhost:5432/scan_explorer_pipeline'
SQLALCHEMY_ECHO = False
# When 'True', no events are emitted to the broker via the webhook
TESTING_MODE = True
# When 'True', it converts all the asynchronous calls into synchronous,
# thus no need for rabbitmq, it does not forward to master
# and it allows debuggers to run if needed:
CELERY_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
