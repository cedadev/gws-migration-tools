import sys
import argparse


from gws_migration_tools import gws
from gws_migration_tools.migration_request_lib \
    import MigrateRequestsManager, NotInitialised


def parse_args(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description='request data migration')

    parser.add_argument('directory',
                        help='directory to migrate')

    return parser.parse_args()


def create_request(args):

    gws_root = gws.get_gws_root_from_path(args.directory)

    rrm = MigrateRequestsManager(gws_root)

    return rrm.create_request(args.directory)


def main():
    
    args = parse_args()

    try:
        req = create_request(args)
    except Exception as exc:
        print(("Creation of request failed with the following error:\n {}"
               ).format(exc))
        sys.exit(1)

    print("created request")
    req.dump()
