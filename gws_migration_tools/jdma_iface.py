import os
import time
import re
import sys

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
            workspace (used for storage allocation) matches the one on which the files are located

        Returns the request ID
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

        return self._resp_to_req_id(resp)
        

    def _resp_to_req_id(self, resp):
        """
        returns the request ID in the response from JDMA, 
        or if status code was not 200, raises an exception with 
        the error.
        """
        try:
            fields = resp.json()
        except ValueError:
            raise JDMAInterfaceError('unparseable response from JDMA')

        status_code = resp.status_code

        if status_code == 200:
            try:
                return fields['request_id']
            except KeyError:
                raise JDMAInterfaceError('no request ID in JDMA response')
            
        elif 'error' in fields:
            raise JDMAInterfaceError('JDMA request failed with HTTP status code {} and message: {}'
                                     .format(status_code, fields['error']))
        else:
            raise JDMAInterfaceError('JDMA request failed with HTTP status code {}'
                                     .format(status_code))


    def _get_workspace(self, path):
        """
        From the path, return the workspace whose allocation will be used
        by the JDMA.  This would exclude any _vol<number> part of the directory
        path.
        """
        gws_root = get_gws_root_from_path(path)
        basename = os.path.basename(os.path.normpath(gws_root))
        return re.sub('_vol[0-9]+$', '', basename)

    

    def _get_batch_id_for_path(self, path, must_exist=False):
        id = self._get_batch_id_for_path2(path)
        if id == None and must_exist:
            raise JDMAInterfaceError('could not find batch on storage for path {}'.format(path))
        else:
            return id


    def _get_batch_id_for_path2(self, path):
        """
        Look up the batch with label = the supplied path
        and whose location is 'ON_STORAGE'
        """

        workspace = self._get_workspace(path)

        resp = jdma_lib.get_batch(self.username,
                                  workspace=workspace,
                                  label=path)

        if resp.status_code != 200:
            if resp.status_code % 100 == 5:
                sys.stderr.write(('Warning: JDMA responded with status code {} when checking for '
                                  'existing batches. Assuming none found.\n'
                                  ).format(resp.status_code))
            return None

        resp_dict = resp.json()

        if 'migrations' in resp_dict:
            batches = resp_dict['migrations']
        else:
            batches = [resp_dict]
        
        batch_ids = [batch['migration_id'] for batch in batches 
                     if jdma_common.get_batch_stage(batch['stage']) == 'ON_STORAGE']
    
        num_matches = len(batch_ids)

        if num_matches == 0:
            return None

        elif num_matches == 1:
            return batch_ids[0]

        else:
            raise JDMAInterfaceError('found more than one batch on storage for path {} (ids={})'
                                     .format(path,
                                             ','.join(map(str, batch_ids))))
    

    def submit_retrieve(self, params):

        """
        Submit a RETRIEVE job.
        Returns the request ID
        """

        orig_path = os.path.normpath(params.get('orig_path'))
        new_path = os.path.normpath(params.get('new_path') or orig_path)
        
        batch_id = self._get_batch_id_for_path(orig_path, must_exist=True)

        resp = jdma_lib.download_files(            
            self.username,
            batch_id=batch_id,
            target_dir=new_path,
            credentials=self.credentials)
            
        return self._resp_to_req_id(resp)


    def submit_delete(self, params):
        
        orig_path = os.path.normpath(params.get('orig_path'))

        batch_id = self._get_batch_id_for_path(orig_path, must_exist=True)
        
        resp = jdma_lib.delete_batch(self.username,
                                     batch_id,
                                     storage=self.storage_type,
                                     credentials=self.credentials)
         
        return self._resp_to_req_id(resp)
            
        


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

        if resp.status_code // 100 == 5:
            raise JDMAInterfaceError("JDMA query failure checking request {}"
                                     .format(message, ext_id))            

        ext_req = resp.json()

        if resp.status_code != 200:
            try:
                message = "JDMA error '{}'".format(ext_req['error'])
            except KeyError:
                message = 'JDMA unknown error'

            raise JDMAInterfaceError("{} when checking request {}"
                                     .format(message, ext_id))

        stage = ext_req['stage']
        stage_name = jdma_common.get_request_stage(stage)

        message = 'JDMA reported stage {} when checked at {}'.format(stage_name,
                                                                     time.asctime())

        if stage_name in ('PUT_COMPLETED',
                          'GET_COMPLETED',
                          'DELETE_COMPLETED'):
            succeeded = True
        
        elif stage_name == 'FAILED':
            succeeded = False

        else:
            succeeded = None

        return { 'succeeded': succeeded,
                 'message': message}
        
        

jdma_iface = JDMAInterface()
