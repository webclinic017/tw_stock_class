import os
import datetime

BASE_DIR = '/tmp'
# os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def git_init():
    os.chdir(BASE_DIR)
    os.system("git pull origin master")


def git_commit():
    git_init()
    now = datetime.datetime.now()
    now = str(now.strftime("%Y-%m-%d %H:%M"))
    os.system("git add data/")
    os.system("git commit -m '%s update data'" % now)
    os.system("git push -u origin master")
    info = "Finish!git push successfully"
    return info
