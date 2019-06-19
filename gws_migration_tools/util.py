import os
import pwd


def get_user_login_name():
    "get a user login name"
    return pwd.getpwuid(os.getuid()).pw_name
