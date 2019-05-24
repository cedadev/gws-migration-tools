import sys
import argparse


from gws_migration_tools import gws
from gws_migration_tools.migration_request_lib import \
    MigrateRequestsManager, RetrieveRequestsManager, NotInitialised


def parse_args(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description=('create directories required for migration requests '
                     'for a group workspace '
                     '(to be run by GWS manager)'))

    parser.add_argument('gws',
                        help='path to group workspace')

    return parser.parse_args()


def main():

    args = parse_args()

    gws_root = gws.get_gws_root_from_path(args.gws)

    if not gws.am_gws_manager(gws_root):
        print("Exiting")
        sys.exit(1)

    for cls in MigrateRequestsManager, RetrieveRequestsManager:
        mgr = cls(gws_root)
        try:
            mgr.initialise()
        except (OSError, NotInitialised):
            print("Initialisation failed")
            sys.exit(1)
    print("created control files/directories under {}".format(mgr.base_dir))
