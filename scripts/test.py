import os
import sys
import yaml

import numpy as np

sys.path.append(os.getcwd())

from vlapy import inspect

# read yaml configuration file
with open("input/config.yaml", "r") as file:
    conf = yaml.safe_load(file)

root = conf["root"]
obs = conf["obs list"][0]
ms = os.path.join(root, f"{obs}/{obs}_hanning.ms/")
ntimes_file = os.path.join(root, f"{obs}/output/ntimes.txt")
ntimes = np.loadtxt(ntimes_file).astype(int)

print(ntimes)

path = os.path.join(root, f"{obs}/output/z_score.h5")
inspect.get_mod_z_score_data(ms, masked=False, ntimes=ntimes, path=path)
