import os
import time

from jdma_client import jdma_lib, jdma_common

from gws_migration_tools.util import get_user_login_name
from gws_migration_tools.gws import get_gws_root_from_path


class JDMAInterfaceError(Exception):
    pass


class JDMAInterface(object):

    def __init__(self, username=None):
        if not username:
            username = get_user_login_name()
        self.username = username
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
        
        path = os.path.normpath(params.get('path'))

        workspace = self._get_workspace(path)

        batch_id = self._get_batch_id_for_path(path)
        if batch_id != None:
            raise JDMAInterfaceError(('Path {} has already been migrated (as batch ID: {})'
                                      ).format(path, batch_id))

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

    
    def _get_batch_id_for_path(self, path):

        workspace = self._get_workspace(path)

        resp = jdma_lib.get_batch(self.username,
                                  workspace=workspace,
                                  label=path)

        if resp.status_code == 404:
            return None
        else:
            return resp.json()['migration_id']

    
    def submit_retrieve(self, params):

        orig_path = os.path.normpath(params.get('orig_path'))
        new_path = os.path.normpath(params.get('new_path') or orig_path)
        
        batch_id = self._get_batch_id_for_path(orig_path)
        
        resp = jdma_lib.download_files(            
            self.username,
            batch_id=batch_id,
            target_dir=new_path,
            credentials=self.credentials)
            
        return resp.json()['request_id']


    def check(self, params):
        """
        Check status of a request.
        Returns a dictionary with:
        
           key 'succeeded' with value: True / False if completed/failed, 
                                       or None if still in progress
           and maybe a key 'message' with a message
        """

        ext_id = params.get('external_id')
        if not ext_id:
            raise JDMAInterfaceError('attempt to check a request that has not '
                                     'yet been submitted')

        resp = jdma_lib.get_request(self.username, req_id=ext_id)

        ext_req = resp.json()

        stage = ext_req['stage']
        stage_name = jdma_common.get_request_stage(stage)

        message = 'JDMA reported stage {} when checked at {}'.format(stage_name,
                                                                     time.asctime())

        if stage_name in ('PUT_COMPLETED',
                          'GET_COMPLETED'):
            succeeded = True
        
        elif stage_name == 'FAILED':
            succeeded = False

        else:
            succeeded = None

        return { 'succeeded': succeeded,
                 'message': message}
        
        

jdma_iface = JDMAInterface()
