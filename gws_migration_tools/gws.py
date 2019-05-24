import os
import sys


class NotAGroupWorkspace(Exception):
    pass


def get_gws_root_from_path(path):
    
    """
    Given a path, return the top-level path to the containing GWS.  The 
    path does not have to exist, but the GWS must do so.
    """

    if path.startswith("/gws/"):
        depth = 4

    elif path.startswith("/group_workspaces/"):
        depth = 3

    elif '_USE_TEST_GWS' in os.environ and path.startswith("/tmp/"):
        depth = 2

    else:
        raise NotAGroupWorkspace(path)

    elements = os.path.normpath(path).split("/")
    
    if len(elements) < depth:
        raise NotAGroupWorkspace(path)
    
    gws_path = os.path.join("/", *elements[:depth + 1])

    if not os.path.isdir(gws_path):
        raise NotAGroupWorkspace(path)

    return gws_path


def am_gws_manager(gws_root):
    """
    Returns boolean, True if user is GWS manager.
    This implementation just asks checks the ownership of any existing .mngr
    directory, or if none exists, then asks the user to confirm.
    (It could be later replaced by something that looks it up from somewhere,
    but that is probably not necessary.)
    """

    try:
        stat = os.stat(get_mgr_directory(gws_root))
        return stat.st_uid == os.getuid()

    except FileNotFoundError:
        print(("If you are the manager of GWS {}, type 'Y' to confirm."
           ).format(gws_root))
        return sys.stdin.readline().upper().startswith('Y')


def get_mgr_directory(gws_root):
    return os.path.join(gws_root, '.mngr')
