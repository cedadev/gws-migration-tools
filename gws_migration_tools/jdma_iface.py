import os

from jdma_client import jdma_lib, jdma_common

from gws_migration_tools.util import get_user_login_name
from gws_migration_tools.gws import get_gws_root_from_path



class JDMAInterface(object):

    def __init__(self, username=None):
        if not username:
            username = get_user_login_name()
        self._username = username
        self._set_storage_params()


    def _set_storage_params(self):
        self.storage_type = 'elastictape'
        self.credentials = {}


    def submit_migrate(self, params):
        """
        Submit a MIGRATE job.
        Wraps jdma_lib with the following constraints:
            filelist consists of the single directory to be uploaded
            label is this directory to be uploaded
        """
        
        path = os.normpath(params['path'])

        workspace = self._get_workspace(path)

        resp = jdma_lib.upload_files(
            self.username,
            filelist=[path],
            request_type='MIGRATE',
            storage=self.storage_type,
            label=path,
            credentials=self.credentials,
            workspace=workspace)
        
        return resp.json()['request_id']


    def _get_workspace(self, path):
        gws_root = get_gws_root_from_path(path)
        return os.path.basename(os.path.normpath(gws_root))

    
    def submit_retrieve(self, params):

        orig_path = os.normpath(params['orig_path'])
        new_path = os.normpath(params['new_path'])

        if not new_path:
            new_path = orig_path
        
        workspace = self._get_workspace(orig_path)

        batches = get_batch(self.username,
                            workspace=workspace,
                            label=orig_path)

        batch_id = batches[0].....  # FIXME
        
        resp = jdma_lib.download_files(            
            self.username,
            batch_id=batch_id,
            target_dir=new_path,
            credentials=self.credentials)
            
        return resp.json()['request_id']


    def check(self, params):
        
        ext_id = params['external_id']

        ext_req = jdma_req.get_request(self.username, req_id=ext_id)
        stage = ext_req['stage']
        stage_name = jdma_common.get_request_stage(stage)

        if stage_name in ('PUT_COMPLETED',
                          'GET_COMPLETED'):
            return True
        
        elif stage_name == 'FAILED':
            return False

        else:
            return None
        
        

jdma_iface = JDMAInterface()
