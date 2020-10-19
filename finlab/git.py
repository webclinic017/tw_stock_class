import os
import datetime

# BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), "../../tw_stock_class"))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def git_commit():
    now = datetime.datetime.now()
    now = str(now.strftime("%Y-%m-%d %H:%M"))
    os.chdir(BASE_DIR)
    os.system('git pull')
    diff_msg = os.popen('git diff data/').read()
    if diff_msg == '':
        return 'Finish!data was not diff'
    os.system("git add data/")
    os.system("git commit -m '%s update data'" % now)
    os.system('git push -u origin master')
    info = 'Finish!git push successfully'
    return info
