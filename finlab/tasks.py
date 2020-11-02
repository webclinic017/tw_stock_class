import logging
from tw_stock_class.finlab.crawler import *
from tw_stock_class.finlab.git import git_commit

logging.basicConfig(level=logging.INFO)


def crawler_task():
    df = TwStockClassPickler().download_all()
    git_commit()
    return df


def git_commit_task():
    df = git_commit()
    return df
