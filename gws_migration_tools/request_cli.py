import os
import sys
import argparse


from gws_migration_tools import gws
from gws_migration_tools.migration_request_lib import RequestsManager, RequestStatus


def parse_args_migration(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description='request data migration')

    parser.add_argument('directory',
                        help='directory to migrate')

    return parser.parse_args()


def parse_args_retrieval(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description='request retrieval of migrated data')

    parser.add_argument('orig_dir',
                        help='directory which was migrated')

    parser.add_argument('dest_dir',
                        nargs='?',
                        help=('optional directory to retrieve to '
                              '(by default, retrieve to original location)'))

    return parser.parse_args()


def parse_args_deletion(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description='request deletion of offline copy of data')

    parser.add_argument('directory',
                        help='original directory that was migrated')

    return parser.parse_args()


def parse_args_withdraw(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description='withdraw request to migrate/retrieve data')

    parser.add_argument('gws',
                        help='path to group workspace')

    parser.add_argument('id',
                        type=int,
                        help='request id')

    return parser.parse_args()


def parse_args_list(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description=('list migration and retrieval requests '))

    parser.add_argument('gws',
                        help='path to group workspace')

    parser.add_argument('-a', '--all-users',
                        help='show requests for all users',
                        action='store_true')

    parser.add_argument('-c', '--current',
                        help='only show requests with status NEW or SUBMITTED',
                        action='store_true')

    return parser.parse_args()


def create_migration_request(args):

    # using exists rather than isdir - a file will actually work, although the usage
    # message doesn't advertise that migrating a file at a time is possible
    if not os.path.exists(args.directory):
        raise ValueError("path {} does not exist".format(args.directory))

    gws_root = gws.get_gws_root_from_path(args.directory)
    rm = RequestsManager(gws_root)
    req = rm.create_migration_request({'path': args.directory})
    print("created request")
    req.dump()


def create_retrieval_request(args):

    gws_root = gws.get_gws_root_from_path(args.orig_dir)
    
    if (args.dest_dir and 
        gws.get_gws_root_from_path(args.dest_dir) != gws_root):
        raise Exception("You cannot restore to a different Group Workspace.")
    
    dest_dir = args.dest_dir or args.orig_dir
    if os.path.exists(dest_dir) and not (os.path.isdir(dest_dir) and not os.listdir(dest_dir)):
        raise ValueError("destination directory {} exists and is not an empty directory"
                         .format(dest_dir))

    rm = RequestsManager(gws_root)

    req = rm.create_retrieval_request({'orig_path': args.orig_dir,
                                       'new_path': args.dest_dir})
    print("created request")
    req.dump()

    
def create_deletion_request(args):

    gws_root = gws.get_gws_root_from_path(args.directory)
    rm = RequestsManager(gws_root)
    req = rm.create_deletion_request({'orig_path': args.directory})
    print("created request")
    req.dump()


def withdraw_request(args):

    gws_root = gws.get_gws_root_from_path(args.gws)    
    rm = RequestsManager(gws_root)
    rm.withdraw(args.id)
    print("withdrew request id={}".format(args.id))


def list_requests(args):

    gws_root = gws.get_gws_root_from_path(args.gws)

    statuses = None
    if args.current:
        statuses = (RequestStatus.NEW, RequestStatus.SUBMITTED)

    mgr = RequestsManager(gws_root)
    for req in mgr.scan(all_users=args.all_users,
                        statuses=statuses):
        req.dump()
    


def common_wrapper(func, args):
    try:
        func(args)
    except Exception as exc:
        print(("Failed with the following error:\n {}"
               ).format(exc))
        sys.exit(1)


def main_migration():
    args = parse_args_migration()
    common_wrapper(create_migration_request, args)


def main_retrieval():
    args = parse_args_retrieval()
    common_wrapper(create_retrieval_request, args)


def main_deletion():
    args = parse_args_deletion()
    common_wrapper(create_deletion_request, args)


def main_withdraw():
    args = parse_args_withdraw()
    common_wrapper(withdraw_request, args)
    

def main_list():
    args = parse_args_list()
    common_wrapper(list_requests, args)
