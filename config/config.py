import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')

from mltb.struct import Struct
from mltb.utils.utilities import *

import pathlib
config = Struct(str(pathlib.Path(__file__).parent) + '/config.yml')
config.project.folder = str(pathlib.Path(__file__).parents[1])




