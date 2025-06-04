import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')
from mltb.struct import Struct
from mltb.utils.utilities import *
import pathlib
import os
import logging

# setup config
config = Struct(str(pathlib.Path(__file__).parent) + '/config.yml')
config.project.folder = str(pathlib.Path(__file__).parents[1])

# setup logging
logging_fpath = r"U:\Research\Projects\sef\encast\NEM\logging\logging_{}.log"
logging_fpath = logging_fpath.format(pd.Timestamp.today().strftime('%Y%m%d'))
def setup_logger(name=__name__, log_file=None, level=logging.DEBUG):
    log_file = log_file if log_file is not None else logging_fpath
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:  # Avoid duplicate handlers
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)

        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        # Add handlers
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger

