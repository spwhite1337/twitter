import os
import logging

# Setup logs
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get root dir
ROOT_DIR = os.getcwd()


class Config(object):
    ROOT_DIR = ROOT_DIR
    DATA_DIR = os.path.join(ROOT_DIR, 'data')
    RESULTS_DIR = os.path.join(ROOT_DIR, 'results')
    version = 'v1'
