import os
import pwd


def get_user_login_name():
    "get a user login name"
    return pwd.getpwuid(os.getuid()).pw_name


def ensure_parent_dir_exists(path):
    parent = os.path.dirname(os.path.normpath(path))
    if not os.path.isdir(parent):
        ensure_parent_dir_exists(parent)
        os.mkdir(parent)
    
