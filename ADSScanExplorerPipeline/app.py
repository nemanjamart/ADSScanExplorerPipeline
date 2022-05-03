from adsputils import ADSCelery
from .models import *

class ADSScanExplorerPipeline(ADSCelery):
    def attempt_recovery(self, task, args=None, kwargs=None, einfo=None, retval=None):
        """
        If task fails after 3 attempts...
        """
        #task.apply_async(args=args, kwargs=kwargs)
        pass
