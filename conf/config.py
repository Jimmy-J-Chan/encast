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
base_fpath1 = r"C:\Users\n8871191\OneDrive - Queensland University of Technology\Documents\schd_tasks\logging" # qut laptop/VM
base_fpath2 = r"C:\Users\Jimmy\OneDrive - Queensland University of Technology\Documents\schd_tasks\logging" # home pc
base_fpath3 = str(pathlib.Path(__file__).parents[2]) # default into root folder
base_fpath3 = base_fpath3 if config.project.folder.endswith('_internal') else config.project.folder
base_fpath = base_fpath1 if os.path.isdir(base_fpath1) else (base_fpath2 if os.path.isdir(base_fpath2) else base_fpath3)
logging_fpath = base_fpath + "\logging_{}.log"
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

def chunks(lst, n, enum=True):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        if enum:
            yield i, lst[i:i + n]
        else:
            yield lst[i:i + n]

