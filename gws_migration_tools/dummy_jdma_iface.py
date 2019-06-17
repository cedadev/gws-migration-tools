import sys
import random

def check(params):
    print("Check placeholder: ", params)
    print("did it succeed, enter Y or N, or hit enter if still running")
    line = sys.stdin.readline().upper()
    if line.startswith("Y"):
        return True
    if line.startswith("N"):
        return False
    

def submit_migrate(params):
    print("submit migrate placeholder", params)
    return _gen_dummy_submit()


def submit_retrieve(params):
    print("submit retrieve placeholder", params)
    return _gen_dummy_submit()


def _gen_dummy_submit():
    ext_id = random.randint(1000, 2000)
    print("dummy external ID = {}".format(ext_id))
    return ext_id
