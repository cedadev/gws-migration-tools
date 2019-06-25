import os
import pwd
import sys
import traceback


def get_user_login_name():
    "get a user login name"
    return pwd.getpwuid(os.getuid()).pw_name


def ensure_parent_dir_exists(path):
    parent = os.path.dirname(os.path.normpath(path))
    if not os.path.isdir(parent):
        ensure_parent_dir_exists(parent)
        os.mkdir(parent)
    

def get_traceback():
    exc, msg, tb = sys.exc_info()
    if exc:
        return '\n'.join(traceback.format_tb(tb))
    else:
        return None
