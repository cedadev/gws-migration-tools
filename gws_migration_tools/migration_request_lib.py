import os
from enum import Enum
import datetime
import re
import json

from gws_migration_tools.util import get_user_login_name, ensure_parent_dir_exists
from gws_migration_tools.gws import get_mgr_directory

#import gws_migration_tools.dummy_jdma_iface as jdma_iface   # dummy code only

if '_USE_TEST_STORAGE' in os.environ:
    from gws_migration_tools.jdma_iface_test import jdma_iface    
else:
    from gws_migration_tools.jdma_iface import jdma_iface


class RequestStatus(Enum):
    NEW = 1
    SUBMITTING = 2
    SUBMITTED = 3
    DONE = 4
    FAILED = 5
    WITHDRAWN = 6


all_statuses = [v for v in RequestStatus.__members__.values()]

finished_statuses = [RequestStatus.DONE, RequestStatus.FAILED, RequestStatus.WITHDRAWN]


class BadFileName(Exception):
    pass


class BadFileContent(Exception):
    pass


class NotInitialised(Exception):
    def __str__(self):
        return ('Migrations have not yet been initialised for this group '
                'workspace by the GWS manager')


def _make_tmp_path(path):
    dirname = os.path.dirname(path)
    filename = os.path.basename(path)
    return os.path.join(dirname, '.tmp_' + filename)


def _is_tmp_path(path):
    # path may be the full path or just the filename
    return os.path.basename(path).startswith('.tmp_')


class RequestBase(object):

    def __init__(self, filename, requests_mgr, status, reqid=None, is_archived=False):
        self.filename = filename
        self.requests_mgr = requests_mgr
        self.status = status
        self.is_archived = is_archived
        if reqid == None:
            _, _, reqid, _ = self.requests_mgr.parse_filename(self.filename)
            self.reqid = reqid
        else:
            self.reqid = reqid


    def write(self, params):
        params = params.copy()
        params['request_type'] = self.request_type
        content = self._encode(params)
        path = self._path
        tmp_path = _make_tmp_path(path)

        try:
            with open(tmp_path, "w") as f:
                f.write(content)
            os.chmod(tmp_path, 0o644)
            os.rename(tmp_path, path)

        except OSError as exc:
            try:
                os.remove(tmp_path)
            except:
                pass
            raise exc


    def read(self):
        with open(self._path) as f:
            content = f.read()
        try:
            return self._decode(content)
        except BadFileContent:
            raise BadFileContent('could not parse {}'.format(self._path))


    def set_external_id(self, ext_id):
        return self.set_param('external_id', ext_id)


    def set_message(self, message):
        if message:
            self.set_param('message', message)


    def set_failed(self, message=None):
        self.set_message(message)
        self.set_status(RequestStatus.FAILED)


    def set_param(self, key, value):
        params = self.read()
        params[key] = value
        self.write(params)
        

    def dump(self):
        print(self)
        content = self.read()
        self._dump(content)
        message = content.get('message')
        if message:
            print(message)
        print("")


    def set_status(self, new_status):
        if self.is_archived:
            raise ValueError("status cannot be changed for archived request")
        self.requests_mgr.move_request_file(
            self.filename, self.status, new_status)
        self.status = new_status


    def archive(self):
        if self.status not in finished_statuses:
            raise ValueError("request cannot be archived while still in progress : {}"
                             .format(self))
        self.is_archived=True
        self.requests_mgr.archive_request_file(self.filename, self.status)


    @property
    def _path(self):
        return self.requests_mgr.get_request_file_path(self.filename,
                                                       self.status,
                                                       self.is_archived)

    @property
    def date(self):
        _, _, _, date = self.requests_mgr.parse_filename(self.filename)
        return date


    
    def _encode(self, params):
        self._check_params(params)
        return json.dumps(params)


    def _decode(self, content):
        d = json.loads(content)
        self._check_params(d)
        return d
        

    def _check_params(self, params):
        for key in self._compulsory_params:
            if key not in params:
                raise TypeError("compulsory request parameter {} missing"
                                .format(key))
        
    
    def __str__(self):
        user, request_type, reqid, date = \
            self.requests_mgr.parse_filename(self.filename)
        return '<{}{} request: user={} id={} date={} status={}>'.format(
            ('archived ' if self.is_archived else ''),
            request_type,
            user, reqid, date, 
            self.status.name)


    def __repr__(self):
        return '{}({}, {}, RequestStatus.{})'.format(
            self.__class__.__name__,
            repr(self.filename),
            repr(self.requests_mgr),
            self.status.name)


    def _encode_int_or_none(self, i):
        if i == None:
            return '-'
        else:
            return str(i)


    def _decode_int_or_none(self, s):
        if s == '-':
            return None
        else:
            return int(s)


    def claim_and_submit(self):
        self.set_status(RequestStatus.SUBMITTING)
        try:
            self.submit()            
            self.set_status(RequestStatus.SUBMITTED)
            return "submitted: {}".format(self)
        except Exception as exc:
            self.set_failed("request was not submitted because: {}".format(exc))
            raise exc

    
    def monitor(self):
        status = self.check()  # True, False, or None
        succeeded = status['succeeded']
        message = status.get('message')
        self.set_message(message)
        if succeeded == True:
            message = "succeeded: {}".format(self)
            self.set_status(RequestStatus.DONE)
        elif succeeded == False:
            message = "failed: {}".format(self)
            self.set_status(RequestStatus.FAILED)
        else:
            message = "still waiting: {}".format(self)
        return message
        

    def check(self):
        params = self.read()
        return jdma_iface.check(params)


class MigrationRequest(RequestBase):

    request_type = 'migration'

    _compulsory_params = ['path']


    def _dump(self, d):
        print(" path to migrate: {}".format(d.get('path')))
        ext_id = d.get('external_id')
        if ext_id != None:
            print(" external ID: {}".format(ext_id))


    def submit(self):
        params = self.read()
        external_id = jdma_iface.submit_migrate(params)
        self.set_external_id(external_id)        


class RetrievalRequest(RequestBase):

    request_type = 'retrieval'

    _compulsory_params = ['orig_path']


    def _dump(self, d):
        print(" original path: {}".format(d.get('orig_path')))
        new_path = d.get('new_path')
        if new_path == None:
            print(" restore to original location")
        else:
            print(" restore to {}".format(new_path))
        ext_id = d.get('external_id')
        if ext_id != None:
            print(" external ID: {}".format(ext_id))


    def submit(self):
        params = self.read()
        external_id = jdma_iface.submit_retrieve(params)
        self.set_external_id(external_id)


class DeletionRequest(RequestBase):

    request_type = 'deletion'

    _compulsory_params = ['orig_path']

    def _dump(self, d):
        print(" original path: {}".format(d.get('orig_path')))
        ext_id = d.get('external_id')
        if ext_id != None:
            print(" external ID: {}".format(ext_id))


    def submit(self):
        params = self.read()
        external_id = jdma_iface.submit_delete(params)
        self.set_external_id(external_id)


_request_class_map = {
    'migration': MigrationRequest,
    'retrieval': RetrievalRequest,
    'deletion': DeletionRequest,
}


class RequestsManager(object):
    
    _dir_lookup = { 
        RequestStatus.NEW: 'new',
        RequestStatus.SUBMITTING: 'submitting',
        RequestStatus.SUBMITTED: 'submitted',
        RequestStatus.DONE: 'done',
        RequestStatus.FAILED: 'failed',
        RequestStatus.WITHDRAWN: 'withdrawn'
        }


    _archive_dir = 'archive'
    _requests_per_archive_dir = 100


    _last_id_file = '.last_id'


    def __init__(self, gws_root):
        self.gws_root = gws_root


    @property
    def base_dir(self):
        return get_mgr_directory(self.gws_root)


    def get_dir_for_status(self, status):
        return os.path.join(self.base_dir,
                            self._dir_lookup[status])


    def _create_dir_for_status(self, status):
        path = self.get_dir_for_status(status)
        if not os.path.isdir(path):
            os.makedirs(path)
            if status in (RequestStatus.NEW, RequestStatus.WITHDRAWN):
                os.chmod(path, 0o1777)
        else:
            print("{} already exists".format(path))


    def initialise(self):
        for status in all_statuses:
            self._create_dir_for_status(status)
        if not os.path.exists(self._last_id_path):
            self._write_last_id(0)
        os.chmod(self._last_id_path, 0o666)
        self._check_initialised()


    def _check_initialised(self):
        for status in all_statuses:
            if not os.path.exists(self.get_dir_for_status(status)):
                self._not_initialised()
        if not os.path.exists(self._last_id_path):
                self._not_initialised()


    def _not_initialised(self):
        raise NotInitialised(self.gws_root)
        

    _fn_matcher = re.compile(
        '(?P<user>[^-]+)-(?P<request_type>[^-]+)-(?P<id>[0-9]+)-'
        '(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})$'
        ).match


    def parse_filename(self, filename):
        m = self._fn_matcher(filename)
        if not m:
            raise BadFileName("cannot parse {}".format(filename))
        user = m.group('user')
        request_type = m.group('request_type')
        reqid = int(m.group('id'))
        date = datetime.date(int(m.group('year')),
                             int(m.group('month')),
                             int(m.group('day')))
        return user, request_type, reqid, date


    def make_filename(self, user, request_type, reqid, date=None):
        if date == None:
            date = datetime.date.today()
        return "{}-{}-{}-{:04}-{:02}-{:02}".format(user, 
                                                   request_type, 
                                                   reqid, 
                                                   date.year,
                                                   date.month,
                                                   date.day)


    def withdraw(self, reqid):
        self._check_initialised()
        req = self.get_by_id(reqid)

        if req.status != RequestStatus.NEW:
            raise Exception(('Withdraw only supported for status NEW.'
                             ' Current status = {}').format(req.status.name))
            
        req.set_status(RequestStatus.WITHDRAWN)


    def get_by_id(self, reqid, **kwargs):
        reqs = self.scan(reqid=reqid, **kwargs)
        if len(reqs) != 1:
            raise Exception("did not find exactly 1 matching request")
        return reqs[0]
        

    def scan(self,
             statuses=None, request_types=None,
             reqid=None, all_users=False,
             include_archived=False):

        self._check_initialised()

        if all_users:
            user = None
        else:
            user = get_user_login_name()

        if statuses == None:
            statuses = all_statuses
        
        reqs = []

        for status in statuses:
            dir_path = self.get_dir_for_status(status)
            for filename, is_archived in self._scan_dir(dir_path,
                                                        include_archived=include_archived):
                if _is_tmp_path(filename):
                    continue

                req_user, request_type, req_id, req_date = \
                                             self.parse_filename(filename)
                if reqid != None and req_id != reqid:
                    continue
                if user != None and req_user != user:
                    continue

                if request_types != None and request_type not in request_types:
                    continue

                request_class = _request_class_map[request_type]

                request = request_class(filename,
                                        self,
                                        status,
                                        is_archived=is_archived)
                reqs.append(request)

        reqs.sort(key=lambda req: req.reqid)
        return reqs


    def _scan_dir(self, path, include_archived=False):
        """
        iterable which yields (filename, is_archived)
        """

        for filename in os.listdir(path):
            # check it is not the archive subdir
            # (if necessary could also do os.path.isfile test but that 
            # is more file metadata I/O on GWS for sake of files that might
            # get filtered out anyway, so just use the filename for this test)
            if filename != self._archive_dir:
                yield (filename, False)

        if include_archived:
            archived_reqs_root = os.path.join(path, self._archive_dir)
            if os.path.isdir(archived_reqs_root):
                for dirpath, dirnames, filenames in os.walk(archived_reqs_root):
                    for filename in filenames:
                        yield (filename, True)


    def get_request_file_path(self, filename, status, is_archived):

        status_dir = self.get_dir_for_status(status)

        if is_archived:
            _, _, req_id, _ = self.parse_filename(filename)
            archive_subdir = os.path.join(self._archive_dir,
                                          str((req_id - 1) // self._requests_per_archive_dir + 1))
            return os.path.join(status_dir, archive_subdir, filename)
            
        else:
            return os.path.join(status_dir, filename)


    def archive_request_file(self, filename, status):
        old_path = self.get_request_file_path(filename, status, False)
        new_path = self.get_request_file_path(filename, status, True)
        ensure_parent_dir_exists(new_path)
        os.rename(old_path, new_path)


    def move_request_file(self, filename, old_status, new_status):
        """
        move a request file - only valid for a request that has not been archived
        """
        old_path = self.get_request_file_path(filename, old_status, False)
        new_path = self.get_request_file_path(filename, new_status, False)
        os.rename(old_path, new_path)

        
    @property
    def _last_id_path(self):
        return os.path.join(self.base_dir, self._last_id_file)


    def _write_last_id(self, reqid):
        with open(self._last_id_path, "w") as f:
            f.write('{}\n'.format(reqid))


    def _read_last_id(self):
        with open(self._last_id_path) as f:
            last_id = int(f.readline())
        return last_id


    def _get_next_id(self):
        last_id = self._read_last_id()
        next_id = last_id + 1
        self._write_last_id(next_id)
        return next_id


    def create_request(self, request_class, *args, **kwargs):

        self._check_initialised()

        reqid = self._get_next_id()
        user = get_user_login_name()
        request_type = getattr(request_class, 'request_type')
        filename = self.make_filename(user, request_type, reqid)
        request = request_class(filename,
                                self,
                                RequestStatus.NEW,
                                reqid=reqid)
        request.write(*args, **kwargs)
        return request
        

    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            repr(self.gws_root))


    def create_migration_request(self, *args, **kwargs):
        return self.create_request(MigrationRequest, *args, **kwargs)

    def create_retrieval_request(self, *args, **kwargs):
        return self.create_request(RetrievalRequest, *args, **kwargs)
    
    def create_deletion_request(self, *args, **kwargs):
        return self.create_request(DeletionRequest, *args, **kwargs)
    

if __name__ == '__main__':
    
    r = RequestsManager('/tmp/mygws')
    r.initialise()
    print(r.get_dir_for_status(RequestStatus.NEW))

