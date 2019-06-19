import argparse
import datetime

from gws_migration_tools import gws
from gws_migration_tools.migration_request_lib \
    import RequestsManager, finished_statuses


def parse_args(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description=('archive finished requests more than a given number of days old)'))

    parser.add_argument('days',
                        help='minimum number of days since original submission',
                        type=int
                    )

    parser.add_argument('gws',
                        help='path to group workspace',
                        nargs='+'
                    )

    args = parser.parse_args()

    return args


def main():

    args = parse_args()

    today = datetime.date.today()
    archive_up_to = today - datetime.timedelta(days=args.days)

    for gws_path in args.gws:
        gws_root = gws.get_gws_root_from_path(gws_path)

        if not gws.am_gws_manager(gws_root):
            print("Skipping group workspace {} - it seems you are not the GWS manager".format(gws_root))
            continue

        reqs_mgr = RequestsManager(gws_root)

        for req in reqs_mgr.scan(all_users=True,
                                  statuses=finished_statuses):

            if req.date <= archive_up_to:
                print('Archiving {}'.format(req))
                req.archive()

