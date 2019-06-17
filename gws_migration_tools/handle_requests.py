import sys
import argparse

from gws_migration_tools import gws
from gws_migration_tools.migration_request_lib \
    import MigrateRequestsManager, RetrieveRequestsManager, RequestStatus


def parse_args(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description=('interacts with JDMA on behalf of user, and update request statuses '
                     '(to be run by GWS manager, probably as a cron job)'))

    req_types = parser.add_argument_group('request types (select at least one)')

    req_types.add_argument('-m', '--migrate',
                           help='act on migration requests',
                           action='store_true'
                       )

    req_types.add_argument('-r', '--retrieve',
                           help='act on retrieval requests',
                           action='store_true'
                       )

    action_types = parser.add_argument_group('actions types (select at least one)')

    action_types.add_argument('-S', '--submit',
                              help='submit new requests and mark them as in progress',
                              action='store_true'
                          )

    action_types.add_argument('-M', '--monitor',
                              help=('monitor existing requests, '
                                    'and if finished, mark them as done or failed'),
                              action='store_true'
                          )

    parser.add_argument('gws',
                        help='path to group workspace',
                        nargs='+'
                    )

    args = parser.parse_args()

    if not (args.migrate or args.retrieve):
        parser.error('No request types: add --migrate or --retrieve or both')

    if not (args.submit or args.monitor):
        parser.error('No action types: add --submit or --monitor or both')

    return args


class Submit:
    input_status = RequestStatus.NEW
    method = 'claim_and_submit'


class Monitor:
    input_status = RequestStatus.SUBMITTED
    method = 'monitor'


def main():

    args = parse_args()

    request_manager_classes = []
    if args.migrate:
        request_manager_classes.append(SubmitRequestsManager)
    if args.retrieve:
        request_manager_classes.append(RetrieveRequestsManager)

    actions = []
    # monitor before submit (avoids pointlessly checking requests
    # that have only just been submitted)
    if args.monitor:
        actions.append(Monitor)
    if args.submit:
        actions.append(Submit)

    for gws_path in args.gws:
        gws_root = gws.get_gws_root_from_path(gws_path)

        if not gws.am_gws_manager(gws_root):
            print("Skipping group workspace {} - it seems you are not the GWS manager".format(gws_root))
            continue

        for reqs_mgr_class in request_manager_classes:

            reqs_mgr = reqs_mgr_class(gws_root)

            for action in actions:

                reqs = reqs_mgr.scan(all_users=True,
                                     statuses=(action.input_status,))

                reqs.sort(key=lambda req:req.reqid)

                for req in reqs:
                    method = getattr(req, action.method)
                    message = method()
                    if message:
                        print(message)
