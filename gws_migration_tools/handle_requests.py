import argparse

from gws_migration_tools import gws
from gws_migration_tools.migration_request_lib \
    import RequestsManager, RequestStatus


def parse_args(arg_list = None):
    
    parser = argparse.ArgumentParser(
        arg_list,
        description=('interacts with JDMA on behalf of user, and update request statuses '
                     '(to be run by GWS manager, probably as a cron job)'))

    req_types_container = parser.add_argument_group('request types (defaults to all)')

    req_types = req_types_container.add_mutually_exclusive_group()

    req_types.add_argument('-m', '--migrate',
                           help='only act on migration requests',
                           action='store_true'
                       )

    req_types.add_argument('-r', '--retrieve',
                           help='only act on retrieval requests',
                           action='store_true'
                       )

    action_types_container = parser.add_argument_group('action types (defaults to all)')

    action_types = action_types_container.add_mutually_exclusive_group()

    action_types.add_argument('-S', '--submit',
                              help='only submit new requests',
                              action='store_true'
                          )

    action_types.add_argument('-M', '--monitor',
                              help=('only monitor already submitted requests'),
                              action='store_true'
                          )

    parser.add_argument('gws',
                        help='path to group workspace',
                        nargs='+'
                    )

    args = parser.parse_args()

    return args


class Submit:
    name = 'submit'
    input_status = RequestStatus.NEW
    method = 'claim_and_submit'


class Monitor:
    name = 'monitor'
    input_status = RequestStatus.SUBMITTED
    method = 'monitor'


def main():

    args = parse_args()

    request_types = []
    if args.migrate:
        request_types.append('migration')
    elif args.retrieve:
        request_types.append('retrieval')
    else:
        request_types = None  # no filter

    actions = []
    # monitor before submit (avoids pointlessly checking requests
    # that have only just been submitted)
    if args.monitor:
        actions.append(Monitor)
    elif args.submit:
        actions.append(Submit)
    else:
        actions = [Monitor, Submit]

    for gws_path in args.gws:
        gws_root = gws.get_gws_root_from_path(gws_path)

        if not gws.am_gws_manager(gws_root):
            print("Skipping group workspace {} - it seems you are not the GWS manager".format(gws_root))
            continue

        reqs_mgr = RequestsManager(gws_root)

        for action in actions:

            reqs = reqs_mgr.scan(all_users=True,
                                 statuses=(action.input_status,),
                                 request_types=request_types)

            reqs.sort(key=lambda req:req.reqid)

            for req in reqs:
                method = getattr(req, action.method)
                try:
                    message = method()
                    if message:
                        print(message)
                except Exception as err:
                    print("{} of request {}: failed with: {}"
                          .format(action.name, req.reqid, err))

                    raise
