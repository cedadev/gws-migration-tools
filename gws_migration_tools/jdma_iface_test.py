import os

from gws_migration_tools.jdma_iface import JDMAInterface


class JDMAInterfaceTest(JDMAInterface):

    def _set_storage_params(self):
        self.storage_type = 'objectstore'
        self._creds = None


    @property
    def credentials(self):
        if not self._creds:
            creds_file = os.path.join(os.environ['HOME'], '.os_creds')
            self._creds = self._read_creds(creds_file)
        return self._creds


    def _read_creds(self, path):
        with open(path) as f:
            access_key = f.readline().strip()
            secret_key = f.readline().strip()
        return {'access_key': access_key,
                'secret_key': secret_key}


jdma_iface = JDMAInterfaceTest()

