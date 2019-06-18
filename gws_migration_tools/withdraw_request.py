import sys
import argparse


from gws_migration_tools import gws
from gws_migration_tools.migration_request_lib import RequestsManager


def parse_args(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description='withdraw request to migrate/retrieve data')

    parser.add_argument('gws',
                        help='path to group workspace')

    parser.add_argument('id',
                        type=int,
                        help='request id')

    return parser.parse_args()


def main():
    args = parse_args()

    try:
        gws_root = gws.get_gws_root_from_path(args.gws)    
        rm = RequestsManager(gws_root)
        rm.withdraw(args.id)
        print("withdrew request id={}".format(args.id))
    except Exception as exc:
        print(("Withdraw of request {} failed with the following error:\n {}"
               ).format(args.id, exc))
        sys.exit(1)
        
