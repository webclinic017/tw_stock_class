from finlab.crawler import *
from finlab.git import git_commit

logging.basicConfig(level=logging.INFO)


def crawler_task(req):
    df = TwStockClassPickler().download_all()
    git_commit()
    return df
