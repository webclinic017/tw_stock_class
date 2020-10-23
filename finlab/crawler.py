from io import StringIO
import numpy as np
import os
import time
from requests.exceptions import ConnectionError
from requests.exceptions import ReadTimeout
import random
import copy
import io
import json
import requests
import datetime
import pandas as pd
import logging
from tw_stock_class.finlab.git import git_init, BASE_DIR

# Get an instance of a logger
logger = logging.getLogger(__name__)


def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        logger.info('Please install lxml(pip install lxml)')


import_or_install("lxml")


def generate_random_header():
    random_user_agents = {'chrome': [
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36',
        'Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/4E423F',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.116 Safari/537.36 Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.517 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.16 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1623.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36',
        'Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.2 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1467.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1500.55 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.90 Safari/537.36',
        'Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36',
        'Mozilla/5.0 (X11; CrOS i686 3912.101.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.15 (KHTML, like Gecko) Chrome/24.0.1295.0 Safari/537.15',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.14 (KHTML, like Gecko) Chrome/24.0.1292.0 Safari/537.14'],
        'opera': ['Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
                  'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
                  'Mozilla/5.0 (Windows NT 6.0; rv:2.0) Gecko/20100101 Firefox/4.0 Opera 12.14',
                  'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0) Opera 12.14',
                  'Opera/12.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.02',
                  'Opera/9.80 (Windows NT 6.1; U; es-ES) Presto/2.9.181 Version/12.00',
                  'Opera/9.80 (Windows NT 5.1; U; zh-sg) Presto/2.9.181 Version/12.00',
                  'Opera/12.0(Windows NT 5.2;U;en)Presto/22.9.168 Version/12.00',
                  'Opera/12.0(Windows NT 5.1;U;en)Presto/22.9.168 Version/12.00',
                  'Mozilla/5.0 (Windows NT 5.1) Gecko/20100101 Firefox/14.0 Opera/12.0',
                  'Opera/9.80 (Windows NT 6.1; WOW64; U; pt) Presto/2.10.229 Version/11.62',
                  'Opera/9.80 (Windows NT 6.0; U; pl) Presto/2.10.229 Version/11.62',
                  'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52',
                  'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; de) Presto/2.9.168 Version/11.52',
                  'Opera/9.80 (Windows NT 5.1; U; en) Presto/2.9.168 Version/11.51',
                  'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; de) Opera 11.51',
                  'Opera/9.80 (X11; Linux x86_64; U; fr) Presto/2.9.168 Version/11.50',
                  'Opera/9.80 (X11; Linux i686; U; hu) Presto/2.9.168 Version/11.50',
                  'Opera/9.80 (X11; Linux i686; U; ru) Presto/2.8.131 Version/11.11',
                  'Opera/9.80 (X11; Linux i686; U; es-ES) Presto/2.8.131 Version/11.11',
                  'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/5.0 Opera 11.11',
                  'Opera/9.80 (X11; Linux x86_64; U; bg) Presto/2.8.131 Version/11.10',
                  'Opera/9.80 (Windows NT 6.0; U; en) Presto/2.8.99 Version/11.10',
                  'Opera/9.80 (Windows NT 5.1; U; zh-tw) Presto/2.8.131 Version/11.10',
                  'Opera/9.80 (Windows NT 6.1; Opera Tablet/15165; U; en) Presto/2.8.149 Version/11.1',
                  'Opera/9.80 (X11; Linux x86_64; U; Ubuntu/10.10 (maverick); pl) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (X11; Linux i686; U; ja) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (X11; Linux i686; U; fr) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 6.1; U; zh-tw) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 6.1; U; sv) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 6.1; U; en-US) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 6.1; U; cs) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 6.0; U; pl) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 5.2; U; ru) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 5.1; U;) Presto/2.7.62 Version/11.01',
                  'Opera/9.80 (Windows NT 5.1; U; cs) Presto/2.7.62 Version/11.01',
                  'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.13) Gecko/20101213 Opera/9.80 (Windows NT 6.1; U; zh-tw) Presto/2.7.62 Version/11.01',
                  'Mozilla/5.0 (Windows NT 6.1; U; nl; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 Opera 11.01',
                  'Mozilla/5.0 (Windows NT 6.1; U; de; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 Opera 11.01',
                  'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; de) Opera 11.01',
                  'Opera/9.80 (X11; Linux x86_64; U; pl) Presto/2.7.62 Version/11.00',
                  'Opera/9.80 (X11; Linux i686; U; it) Presto/2.7.62 Version/11.00',
                  'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.6.37 Version/11.00',
                  'Opera/9.80 (Windows NT 6.1; U; pl) Presto/2.7.62 Version/11.00',
                  'Opera/9.80 (Windows NT 6.1; U; ko) Presto/2.7.62 Version/11.00',
                  'Opera/9.80 (Windows NT 6.1; U; fi) Presto/2.7.62 Version/11.00',
                  'Opera/9.80 (Windows NT 6.1; U; en-GB) Presto/2.7.62 Version/11.00',
                  'Opera/9.80 (Windows NT 6.1 x64; U; en) Presto/2.7.62 Version/11.00',
                  'Opera/9.80 (Windows NT 6.0; U; en) Presto/2.7.39 Version/11.00'],
        'firefox': ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
                    'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0',
                    'Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0',
                    'Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0',
                    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0',
                    'Mozilla/5.0 (X11; OpenBSD amd64; rv:28.0) Gecko/20100101 Firefox/28.0',
                    'Mozilla/5.0 (X11; Linux x86_64; rv:28.0) Gecko/20100101  Firefox/28.0',
                    'Mozilla/5.0 (Windows NT 6.1; rv:27.3) Gecko/20130101 Firefox/27.3',
                    'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:27.0) Gecko/20121011 Firefox/27.0',
                    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:25.0) Gecko/20100101 Firefox/25.0',
                    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0',
                    'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:24.0) Gecko/20100101 Firefox/24.0',
                    'Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/23.0',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
                    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:23.0) Gecko/20131011 Firefox/23.0',
                    'Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/22.0',
                    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:22.0) Gecko/20130328 Firefox/22.0',
                    'Mozilla/5.0 (Windows NT 6.1; rv:22.0) Gecko/20130405 Firefox/22.0',
                    'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0',
                    'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/21.0.1',
                    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/21.0.1',
                    'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:21.0.0) Gecko/20121011 Firefox/21.0.0',
                    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20130331 Firefox/21.0',
                    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20100101 Firefox/21.0',
                    'Mozilla/5.0 (X11; Linux i686; rv:21.0) Gecko/20100101 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20130514 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.2; rv:21.0) Gecko/20130326 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130401 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130330 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20130401 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20130328 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130401 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130331 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20100101 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 5.0; rv:21.0) Gecko/20100101 Firefox/21.0',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0',
                    'Mozilla/5.0 (Windows NT 6.2; Win64; x64;) Gecko/20100101 Firefox/20.0',
                    'Mozilla/5.0 (Windows x86; rv:19.0) Gecko/20100101 Firefox/19.0',
                    'Mozilla/5.0 (Windows NT 6.1; rv:6.0) Gecko/20100101 Firefox/19.0',
                    'Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/18.0.1',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0)  Gecko/20100101 Firefox/18.0',
                    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0.6'],
        'internetexplorer': [
            'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0;  rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/5.0)',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/4.0; InfoPath.2; SV1; .NET CLR 2.0.50727; WOW64)',
            'Mozilla/5.0 (compatible; MSIE 10.0; Macintosh; Intel Mac OS X 10_7_3; Trident/6.0)',
            'Mozilla/4.0 (Compatible; MSIE 8.0; Windows NT 5.2; Trident/6.0)',
            'Mozilla/4.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/5.0)',
            'Mozilla/1.22 (compatible; MSIE 10.0; Windows 3.1)',
            'Mozilla/5.0 (Windows; U; MSIE 9.0; WIndows NT 9.0; en-US))',
            'Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 7.1; Trident/5.0)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; InfoPath.3; MS-RTC LM 8; .NET4.0C; .NET4.0E)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; chromeframe/12.0.742.112)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; Tablet PC 2.0; InfoPath.3; .NET4.0C; .NET4.0E)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; yie8)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET CLR 1.1.4322; .NET4.0C; Tablet PC 2.0)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; FunWebProducts)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; chromeframe/13.0.782.215)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; chromeframe/11.0.696.57)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) chromeframe/10.0.648.205',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.1; SV1; .NET CLR 2.8.52393; WOW64; en-US)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; chromeframe/11.0.696.57)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/4.0; GTB7.4; InfoPath.3; SV1; .NET CLR 3.1.76908; WOW64; en-US)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.2; SV1; .NET CLR 3.3.69573; WOW64; en-US)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; InfoPath.1; SV1; .NET CLR 3.8.36217; WOW64; en-US)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; .NET CLR 2.7.58687; SLCC2; Media Center PC 5.0; Zune 3.4; Tablet PC 3.6; InfoPath.3)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.2; Trident/4.0; Media Center PC 4.0; SLCC1; .NET CLR 3.0.04320)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 1.1.4322)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; SLCC1; .NET CLR 1.1.4322)',
            'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.0; Trident/4.0; InfoPath.1; SV1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 3.0.04506.30)',
            'Mozilla/5.0 (compatible; MSIE 7.0; Windows NT 5.0; Trident/4.0; FBSMTWB; .NET CLR 2.0.34861; .NET CLR 3.0.3746.3218; .NET CLR 3.5.33652; msn OptimizedIE8;ENUS)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.2; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; Media Center PC 6.0; InfoPath.2; MS-RTC LM 8)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; Media Center PC 6.0; InfoPath.2; MS-RTC LM 8',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; Media Center PC 6.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; InfoPath.3; .NET4.0C; .NET4.0E; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MS-RTC LM 8)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; InfoPath.2)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 3.0)'],
        'safari': [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13+ (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10',
            'Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko ) Version/5.1 Mobile/9B176 Safari/7534.48.3',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; da-dk) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; tr-TR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; ko-KR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; fr-FR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; cs-CZ) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; ja-JP) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; zh-cn) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; zh-cn) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; sv-se) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ko-kr) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; fr-fr) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; es-es) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-us) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-gb) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; de-de) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; sv-SE) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; ja-JP) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; de-DE) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; hu-HU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; de-DE) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; ja-JP) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; it-IT) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; en-us) AppleWebKit/534.16+ (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; fr-ch) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_5; de-de) AppleWebKit/534.15+ (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_5; ar) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Android 2.2; Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; zh-HK) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; tr-TR) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; nb-NO) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; fr-FR) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-TW) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_8; zh-cn) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5']}
    random_headers = [
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (X11; Linux i686 on x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36 OPR/56.0.3051.104'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36 OPR/54.0.2952.64'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0.2) Gecko/20100101 Firefox/58.0.2'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36 OPR/56.0.3051.104'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (X11; Linux i686 on x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36 OPR/57.0.3098.116'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (X11; Linux i686 on x86_64; rv:51.0) Gecko/20100101 Firefox/51.0'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.98 Safari/537.36'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:65.0) Gecko/20100101 Firefox/65.0'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'},
        {'Accept': '*/*', 'Connection': 'keep-alive',
         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1; rv:52.1.0) Gecko/20100101 Firefox/52.1.0'},
    ]
    browser = random.choice(list(random_user_agents.keys()))
    user_agent = random.choice(random_user_agents[browser])
    header = copy.copy(random.choice(random_headers))
    header['User-Agent'] = user_agent
    return header


def find_best_session():
    for i in range(10):
        try:
            logger.info('獲取新的Session 第', i, '回合')
            headers = generate_random_header()
            ses = requests.Session()
            ses.get('https://www.twse.com.tw/zh/', headers=headers, timeout=10)
            ses.headers.update(headers)
            logger.info('成功！')
            return ses
        except (ConnectionError, ReadTimeout) as error:
            logger.info(error)
            logger.info('失敗，10秒後重試')
            time.sleep(10)

    logger.info('您的網頁IP已經被證交所封鎖，請更新IP來獲取解鎖')
    logger.info("　手機：開啟飛航模式，再關閉，即可獲得新的IP")
    logger.info("數據機：關閉然後重新打開數據機的電源")


date_range_record_file = os.path.join('history', 'date_range.pickle')

ses = None


def requests_get(*args1, **args2):
    # get current session
    global ses
    if ses == None:
        ses = find_best_session()

    # download data
    i = 3
    while i >= 0:
        try:
            return ses.get(*args1, timeout=10, **args2)
        except (ConnectionError, ReadTimeout) as error:
            logger.info(error)
            logger.info('retry one more time after 60s', i, 'times left')
            time.sleep(60)
            ses = find_best_session()

        i -= 1
    return pd.DataFrame()


### ----------
###   Helper
### ----------

def otc_date_str(date):
    """將datetime.date轉換成民國曆

    Args:
        date (datetime.date): 西元歷的日期

    Returns:
        str: 民國歷日期 ex: 109/01/01
    """
    return str(date.year - 1911) + date.strftime('%Y/%m/%d')[4:]


def combine_index(df, n1, n2):
    """將dataframe df中的股票代號與股票名稱合併

    Keyword arguments:

    Args:
        df (pandas.DataFrame): 此dataframe含有column n1, n2
        n1 (str): 股票代號
        n2 (str): 股票名稱

    Returns:
        df (pandas.DataFrame): 此dataframe的index為「股票代號+股票名稱」
    """

    return df.set_index(df[n1].astype(str).str.replace(' ', '') + \
                        ' ' + df[n2].astype(str).str.replace(' ', '')).drop([n1, n2], axis=1)


def crawl_benchmark(date):
    date_str = date.strftime('%Y%m%d')
    res = requests_get("https://www.twse.com.tw/exchangeReport/MI_5MINS_INDEX?response=csv&date=" +
                       date_str + "&_=1544020420045")

    # 利用 pandas 將資料整理成表格

    if len(res.text) < 10:
        return pd.DataFrame()

    df = pd.read_csv(StringIO(res.text.replace("=", "")), header=1, index_col='時間')

    # 資料處理

    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    df.index = pd.to_datetime(date.strftime('%Y %m %d ') + pd.Series(df.index))
    df = df.apply(lambda s: s.astype(str).str.replace(",", "").astype(float))
    df = df.reset_index().rename(columns={'時間': 'date'})
    df['stock_id'] = '台股指數'
    return df.set_index(['stock_id', 'date'])


def crawl_capital():
    res = requests_get('https://dts.twse.com.tw/opendata/t187ap03_L.csv', )
    res.encoding = 'utf-8'
    df = pd.read_csv(StringIO(res.text))
    time.sleep(10)
    res = requests_get('https://dts.twse.com.tw/opendata/t187ap03_O.csv')
    res.encoding = 'utf-8'
    df = df.append(pd.read_csv(StringIO(res.text)))

    df['date'] = pd.to_datetime(str(datetime.datetime.now().year) + df['出表日期'].str[3:])
    df.set_index([df['公司代號'].astype(str) + ' ' + df['公司簡稱'].astype(str), 'date'], inplace=True)
    df.index.levels[0].name = '股票名稱'
    return df


def interest():
    res = requests_get('https://www.twse.com.tw/exchangeReport/TWT48U_ALL?response=open_data')
    res.encoding = 'utf-8'
    df = pd.read_csv(StringIO(res.text))

    time.sleep(10)

    res = requests_get('https://www.tpex.org.tw/web/stock/exright/preAnnounce/prepost_result.php?l=zh-tw&o=data')
    res.encoding = 'utf-8'
    df = df.append(pd.read_csv(StringIO(res.text)))

    df['date'] = df['除權息日期'].str.replace('年', '/').str.replace('月', '/').str.replace('日', '')
    df['date'] = pd.to_datetime(str(datetime.datetime.now().year) + df['date'].str[3:])
    df = df.set_index([df['股票代號'].astype(str) + ' ' + df['名稱'].astype(str), 'date'])
    return df


def preprocess(df, date):
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    df.columns = df.columns.str.replace(' ', '')
    df.index.name = 'stock_id'
    df.columns.name = ''
    df['date'] = pd.to_datetime(date)
    df = df.reset_index().set_index(['stock_id', 'date'])
    df = df.apply(lambda s: s.astype(str).str.replace(',', ''))

    return df


def bargin_twe(date):
    datestr = date.strftime('%Y%m%d')

    res = requests_get('https://www.twse.com.tw/fund/T86?response=csv&date=' \
                       + datestr + '&selectType=ALLBUT0999')
    try:
        df = pd.read_csv(StringIO(res.text.replace('=', '')), header=1)
    except:
        logger.error('holiday')
        return pd.DataFrame()

    df = combine_index(df, '證券代號', '證券名稱')
    df = preprocess(df, date)
    return df


def bargin_otc(date):
    datestr = otc_date_str(date)

    url = 'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d=' + datestr + '&s=0,asc'
    res = requests_get(url)
    try:
        df = pd.read_csv(StringIO(res.text), header=1)
    except:
        logger.error('holiday')
        return pd.DataFrame()

    df = combine_index(df, '代號', '名稱')
    df = preprocess(df, date)
    return df


def price_twe(date):
    date_str = date.strftime('%Y%m%d')
    res = requests_get(
        'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + date_str + '&type=ALLBUT0999')

    if res.text == '':
        logger.error('holiday')
        return pd.DataFrame()

    header = np.where(list(map(lambda l: '證券代號' in l, res.text.split('\n')[:500])))[0][0]

    df = pd.read_csv(StringIO(res.text.replace('=', '')), header=header - 1)
    df = combine_index(df, '證券代號', '證券名稱')
    df = preprocess(df, date)
    return df


def price_otc(date):
    datestr = otc_date_str(date)
    link = 'https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d=' + datestr + '&s=0,asc,0'
    res = requests_get(link)
    df = pd.read_csv(StringIO(res.text), header=2)

    if len(df) < 30:
        logger.error('holiday')
        return pd.DataFrame()

    df = combine_index(df, '代號', '名稱')
    df = preprocess(df, date)
    df = df[df['成交筆數'].str.replace(' ', '') != '成交筆數']
    return df


def pe_twe(date):
    datestr = date.strftime('%Y%m%d')
    res = requests_get(
        'https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date=' + datestr + '&selectType=ALL')
    try:
        df = pd.read_csv(StringIO(res.text), header=1)
    except:
        logger.error('holiday')
        return pd.DataFrame()

    df = combine_index(df, '證券代號', '證券名稱')
    df = preprocess(df, date)
    return df


def pe_otc(date):
    datestr = otc_date_str(date)
    res = requests_get(
        'https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=csv&charset=UTF-8&d=' + datestr + '&c=&s=0,asc')
    try:
        df = pd.read_csv(StringIO(res.text), header=3)
        df = combine_index(df, '股票代號', '名稱')
        df = preprocess(df, date)
    except:
        logger.error('holiday')
        return pd.DataFrame()

    return df


def month_revenue(name, date):
    year = date.year - 1911
    month = (date.month + 10) % 12 + 1
    if month == 12:
        year -= 1
    url = 'https://mops.twse.com.tw/nas/t21/%s/t21sc03_%d_%d.html' % (name, year, month)
    logger.info(url)
    res = requests_get(url, verify=False)
    res.encoding = 'big5'

    try:
        dfs = pd.read_html(StringIO(res.text), encoding='big-5')
    except:
        logger.info('MONTH ' + name + ': cannot parse ' + str(date))
        return pd.DataFrame()

    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])

    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = df[list(range(0, 10))]
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]

    df = df.loc[:, ~df.columns.isnull()]
    df = df.loc[~pd.to_numeric(df['當月營收'], errors='coerce').isnull()]
    df = df[df['公司代號'] != '合計']
    df = combine_index(df, '公司代號', '公司名稱')
    df = preprocess(df, datetime.date(date.year, date.month, 10))
    return df.drop_duplicates()


def crawl_split_twe():
    res = requests_get('https://www.twse.com.tw/exchangeReport/TWTAVU?response=csv&_=1537824706232')

    df = pd.read_csv(StringIO(res.text), header=1)
    df = df.dropna(how='all', axis=1).dropna(thresh=3, axis=0)

    def process_date(s):
        return pd.to_datetime(str(datetime.datetime.now().year) + s.str[3:])

    df['停止買賣日期'] = process_date(df['停止買賣日期'])
    df['恢復買賣日期'] = process_date(df['恢復買賣日期'])
    df['股票代號'] = df['股票代號'].astype(int).astype(str)
    df['stock_id'] = df['股票代號'] + ' ' + df['名稱']
    df['date'] = df['恢復買賣日期']
    df = df.set_index(['stock_id', 'date'])

    return df


def crawl_split_otc():
    res = requests_get(
        "https://www.tpex.org.tw/web/stock/exright/decap/decap_download.php?l=zh-tw&d=107/09/21&s=0,asc,0")
    df = pd.read_csv(StringIO(res.text), header=1)
    df = df.dropna(thresh=5, axis=0)
    df['stock_id'] = df['代號'] + ' ' + df['名稱']

    def process_date(s):
        ss = s.astype(int).astype(str)
        return pd.to_datetime(str(datetime.datetime.now().year) + '/' + ss.str[3:5] + '/' + ss.str[5:])

    df['停止買賣日期'] = process_date(df['停止買賣日期'])
    df['恢復買賣日期'] = process_date(df['恢復買賣日期'])
    df['date'] = df['恢復買賣日期']
    df = df.rename(columns={'代號': '股票代號'})
    df = df.set_index(['stock_id', 'date'])
    return df


def crawl_twse_divide_ratio():
    datestr = datetime.datetime.now().strftime('%Y%m%d')
    res = requests_get(
        "https://www.twse.com.tw/exchangeReport/TWT49U?response=csv&strDate=20200101&endDate=" + datestr + "&_=1551532565786")

    df = pd.read_csv(io.StringIO(res.text.replace("=", "")), header=1)

    df = df.dropna(thresh=5).dropna(how='all', axis=1)

    df = df[~df['資料日期'].isnull()]

    # set stock id
    df['stock_id'] = df['股票代號'] + ' ' + df['股票名稱']

    # set dates
    df = df[~df['資料日期'].isnull()]
    years = df['資料日期'].str.split('年').str[0].astype(int) + 1911

    # years.loc[df['資料日期'].str[3] != '年'] = np.nan
    years.loc[df['資料日期'].str.find('年') == -1] = np.nan

    years.loc[years > datetime.datetime.now().year] = np.nan
    years.ffill(inplace=True)
    dates = years.astype(int).astype(str) + '/' + df['資料日期'].str.split('年').str[1].str.replace('月', '/').str.replace(
        '日', '')
    df['date'] = pd.to_datetime(dates, errors='coerce')

    # convert to float
    float_name_list = ['除權息前收盤價', '除權息參考價', '權值+息值', '漲停價格',
                       '跌停價格', '開盤競價基準', '減除股利參考價', '最近一次申報每股 (單位)淨值',
                       '最近一次申報每股 (單位)盈餘']

    df[float_name_list] = df[float_name_list].astype(str).apply(lambda s: s.str.replace(',', '')).astype(float)

    df['twse_divide_ratio'] = df['除權息前收盤價'] / df['開盤競價基準']
    return df.set_index(['stock_id', 'date'])


def crawl_otc_divide_ratio():
    y = datetime.datetime.now().year
    m = datetime.datetime.now().month
    d = datetime.datetime.now().day

    y = str(y - 1911)
    m = str(m) if m > 9 else '0' + str(m)
    d = str(d) if d > 9 else '0' + str(d)

    datestr = '%s/%s/%s' % (y, m, d)
    res_otc = requests_get(
        'https://www.tpex.org.tw/web/stock/exright/dailyquo/exDailyQ_result.php?l=zh-tw&d=109/01/01&ed=' + datestr + '&_=1551594269115')
    df = pd.DataFrame(json.loads(res_otc.text)['aaData'])
    df.columns = ['除權息日期', '代號', '名稱', '除權息前收盤價', '除權息參考價',
                  '權值', '息值', "權+息值", "權/息", "漲停價格", "跌停價格", "開盤競價基準",
                  "減除股利參考價", "現金股利", "每千股無償配股", "現金增資股數", "現金增資認購價",
                  "公開承銷股數", "員工認購股數", "原股東認購數", "按持股比例千股認購"]

    float_name_list = ['除權息前收盤價', '除權息參考價',
                       '權值', '息值', "權+息值", "漲停價格", "跌停價格", "開盤競價基準",
                       "減除股利參考價", "現金股利", "每千股無償配股", "現金增資股數", "現金增資認購價",
                       "公開承銷股數", "員工認購股數", "原股東認購數", "按持股比例千股認購"
                       ]
    df[float_name_list] = df[float_name_list].astype(str).apply(lambda s: s.str.replace(',', '')).astype(float)

    # set stock id
    df['stock_id'] = df['代號'] + ' ' + df['名稱']

    # set dates
    dates = df['除權息日期'].str.split('/')
    dates = (dates.str[0].astype(int) + 1911).astype(str) + '/' + dates.str[1] + '/' + dates.str[2]
    df['date'] = pd.to_datetime(dates)

    df['otc_divide_ratio'] = df['除權息前收盤價'] / df['開盤競價基準']
    return df.set_index(['stock_id', 'date'])


def crawl_twse_cap_reduction():
    datestr = datetime.datetime.now().strftime('%Y%m%d')
    res3 = requests_get(
        "https://www.twse.com.tw/exchangeReport/TWTAUU?response=csv&strDate=20200101&endDate=" + datestr + "&_=1551597854043")
    df = pd.read_csv(io.StringIO(res3.text), header=1)
    df = df.dropna(thresh=5).dropna(how='all', axis=1)
    dates = (df['恢復買賣日期'].str.split('/').str[0].astype(int) + 1911).astype(str) + df['恢復買賣日期'].str[3:]
    df['date'] = pd.to_datetime(dates, errors='coerce')
    df['stock_id'] = df['股票代號'].astype(int).astype(str) + ' ' + df['名稱']
    df.head()

    df['twse_cap_divide_ratio'] = df['停止買賣前收盤價格'] / df['開盤競價基準']

    return df.set_index(['stock_id', 'date'])


def crawl_otc_cap_reduction():
    y = datetime.datetime.now().year
    m = datetime.datetime.now().month
    d = datetime.datetime.now().day

    y = str(y - 1911)
    m = str(m) if m > 9 else '0' + str(m)
    d = str(d) if d > 9 else '0' + str(d)

    datestr = '%s/%s/%s' % (y, m, d)
    res4 = requests_get(
        "https://www.tpex.org.tw/web/stock/exright/revivt/revivt_result.php?l=zh-tw&d=109/01/01&ed=" + datestr + "&_=1551611342446")

    df = pd.DataFrame(json.loads(res4.text)['aaData'])

    name = ['恢復買賣日期', '股票代號', '股票名稱', '最後交易之收盤價格',
            '減資恢復買賣開始日參考價格', '漲停價格', '跌停價格', '開始交易基準價', '除權參考價', '減資源因', '詳細資料']

    float_name_list = ['最後交易之收盤價格', '減資恢復買賣開始日參考價格', '漲停價格', '跌停價格', '開始交易基準價', '除權參考價']
    df.columns = name
    df[float_name_list] = df[float_name_list].astype(str).apply(lambda s: s.str.replace(',', '')).astype(float)
    df['stock_id'] = df['股票代號'] + ' ' + df['股票名稱']
    dates = (df['恢復買賣日期'].astype(str).str[:-4].astype(int) + 1911).astype(str) + df['恢復買賣日期'].astype(str).str[-4:]
    df['date'] = pd.to_datetime(dates)
    df['date'] = pd.to_datetime(dates, errors='coerce')

    # calculate open price
    price = df['開始交易基準價'].copy()
    price[price == 0] = np.nan
    price.fillna(df['減資恢復買賣開始日參考價格'], inplace=True)

    df['otc_cap_divide_ratio'] = df['最後交易之收盤價格'] / price

    return df.set_index(['stock_id', 'date'])


o2tp = {'成交股數': '成交股數',
        '成交筆數': '成交筆數',
        '成交金額(元)': '成交金額',
        '收盤': '收盤價',
        '開盤': '開盤價',
        '最低': '最低價',
        '最高': '最高價',
        '最後買價': '最後揭示買價',
        '最後賣價': '最後揭示賣價',
        }

o2tpe = {
    '殖利率(%)': '殖利率(%)',
    '本益比': '本益比',
    '股利年度': '股利年度',
    '股價淨值比': '股價淨值比',
}

o2tb = {
    '外資及陸資(不含外資自營商)-買進股數': '外陸資買進股數(不含外資自營商)',
    '外資及陸資買股數': '外陸資買進股數(不含外資自營商)',

    '外資及陸資(不含外資自營商)-賣出股數': '外陸資賣出股數(不含外資自營商)',
    '外資及陸資賣股數': '外陸資賣出股數(不含外資自營商)',

    '外資及陸資(不含外資自營商)-買賣超股數': '外陸資買賣超股數(不含外資自營商)',
    '外資及陸資淨買股數': '外陸資買賣超股數(不含外資自營商)',

    '外資自營商-買進股數': '外資自營商買進股數',
    '外資自營商-賣出股數': '外資自營商賣出股數',
    '外資自營商-買賣超股數': '外資自營商買賣超股數',
    '投信-買進股數': '投信買進股數',
    '投信買進股數': '投信買進股數',
    '投信-賣出股數': '投信賣出股數',
    '投信賣股數': '投信賣出股數',

    '投信-買賣超股數': '投信買賣超股數',
    '投信淨買股數': '投信買賣超股數',

    '自營商(自行買賣)-買進股數': '自營商買進股數(自行買賣)',
    '自營商(自行買賣)買股數': '自營商買進股數(自行買賣)',

    '自營商(自行買賣)-賣出股數': '自營商賣出股數(自行買賣)',
    '自營商(自行買賣)賣股數': '自營商賣出股數(自行買賣)',

    '自營商(自行買賣)-買賣超股數': '自營商買賣超股數(自行買賣)',
    '自營商(自行買賣)淨買股數': '自營商買賣超股數(自行買賣)',

    '自營商(避險)-買進股數': '自營商買進股數(避險)',
    '自營商(避險)買股數': '自營商買進股數(避險)',
    '自營商(避險)-賣出股數': '自營商賣出股數(避險)',
    '自營商(避險)賣股數': '自營商賣出股數(避險)',
    '自營商(避險)-買賣超股數': '自營商買賣超股數(避險)',
    '自營商(避險)淨買股數': '自營商買賣超股數(避險)',

}

o2tm = {n: n for n in ['當月營收', '上月營收', '去年當月營收', '上月比較增減(%)', '去年同月增減(%)', '當月累計營收', '去年累計營收',
                       '前期比較增減(%)']}


def merge(twe, otc, t2o):
    t2o2 = {k: v for k, v in t2o.items() if k in otc.columns}
    otc = otc[list(t2o2.keys())]
    otc = otc.rename(columns=t2o2)
    twe = twe[otc.columns & twe.columns]

    return twe.append(otc)


def crawl_price(date):
    dftwe = price_twe(date)
    time.sleep(10)
    dfotc = price_otc(date)
    if len(dftwe) != 0 and len(dfotc) != 0:
        df = merge(dftwe, dfotc, o2tp)
        return df
    else:
        return pd.DataFrame()


def crawl_bargin(date):
    dftwe = bargin_twe(date)
    dfotc = bargin_otc(date)
    if len(dftwe) != 0 and len(dfotc) != 0:
        return merge(dftwe, dfotc, o2tb)
    else:
        return pd.DataFrame()


def crawl_monthly_report(date):
    dftwe = month_revenue('sii', date)
    time.sleep(10)
    dfotc = month_revenue('otc', date)
    if len(dftwe) != 0 and len(dfotc) != 0:
        return merge(dftwe, dfotc, o2tm)
    else:
        return pd.DataFrame()


def crawl_pe(date):
    dftwe = pe_twe(date)
    dfotc = pe_otc(date)
    if len(dftwe) != 0 and len(dfotc) != 0:
        return merge(dftwe, dfotc, o2tpe)
    else:
        return pd.DataFrame()


# BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), "../../tw_stock_class"))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TwStockClassPickler:
    crawlers_list = ['crawl_price', 'crawl_bargin', 'crawl_pe', 'crawl_benchmark', 'crawl_monthly_report']
    crawlers_once_list = ['crawl_twse_divide_ratio', 'crawl_otc_divide_ratio', 'crawl_twse_cap_reduction',
                          'crawl_otc_cap_reduction']

    def download_single(self, func: str, dt=False, once=False):
        if dt is False:
            dt = datetime.datetime.now()
        dir_name = func[func.index('_') + 1:]

        if func != 'crawl_monthly_report':
            file_name = dt.strftime('%Y%m%d')
        else:
            file_name = dt.strftime('%Y%m')

        if once:
            df = eval(func)()
            if df.empty is False:
                df.to_pickle(f'{BASE_DIR}/data/{dir_name}/{dir_name}.pickle')
        else:
            df = eval(func)(dt)
            if df.empty is False:
                df.to_pickle(f'{BASE_DIR}/data/{dir_name}/{file_name}.pickle')
        return df

    def download_all(self, dt=False):
        git_init()
        os.system("git pull origin master")
        if dt is False:
            dt = datetime.date.today()
            dt = datetime.datetime(dt.year, dt.month, dt.day)
        for crawler_name in self.crawlers_list:
            self.download_single(crawler_name, dt)

        for crawler_name in self.crawlers_once_list:
            self.download_single(crawler_name, once=True)
        info = 'Finish!download class data and divide pickle file'
        return info
