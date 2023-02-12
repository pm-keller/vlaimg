#!/DATA/CARINA_3/kel334/miniconda3/envs/vlaimg3.8/bin/python3 
# -*-coding:utf-8 -*-
"""

applycal.py

Created on: 2023/02/08
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Apply calibration to measurement set

"""


import os
import glob
import shutil
import yaml
import casatasks
import casaplotms
import numpy as np

# set display environment variable
from pyvirtualdisplay import Display

display = Display(visible=0, size=(2048, 2048))
display.start()

print("\nAPPLY CALIBRATION")

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)


print("\nflagging summary: applycal_summary_before.npy")
summary_1 = casatasks.flagdata(obs["ms hanning"], mode="summary")
np.save("applycal_summary_before.npy", summary_1)

# specify gain tables
gaintable = []
for table in [
    "opac",
    "rq",
    "antpos",
    "final.K",
    "final.B",
    "final.G2",
    "final_long.G",
    "final_phase.G",
]:
    gaintable.append(f"{obs['name']}.{table}")

print("\napply calibration")
casatasks.applycal(
    obs["ms hanning"],
    gaintable=gaintable,
    interp=["", "", "", "", "nearest,nearestflag", "", "", ""],
    calwt=False,
    parang=True,
    applymode="calflagstrict",
    flagbackup=False,
)

print("\nflagging summary: applycal_summary_after.npy")
summary_2 = casatasks.flagdata(obs["ms hanning"], mode="summary")
np.save("applycal_summary_after.npy", summary_2)

print("\nflagging summary: applycal_summary_detailed.npy")
summary_3 = casatasks.flagdata(
    obs["ms hanning"],
    mode="list",
    inpfile=[
        "spw='0' fieldcnt=True mode='summary' name='AntSpw000",
        "spw='1' fieldcnt=True mode='summary' name='AntSpw001",
        "spw='2' fieldcnt=True mode='summary' name='AntSpw002",
        "spw='3' fieldcnt=True mode='summary' name='AntSpw003",
        "spw='4' fieldcnt=True mode='summary' name='AntSpw004",
        "spw='5' fieldcnt=True mode='summary' name='AntSpw005",
        "spw='6' fieldcnt=True mode='summary' name='AntSpw006",
        "spw='7' fieldcnt=True mode='summary' name='AntSpw007",
        "spw='8' fieldcnt=True mode='summary' name='AntSpw008",
        "spw='9' fieldcnt=True mode='summary' name='AntSpw009",
        "spw='10' fieldcnt=True mode='summary' name='AntSpw010",
        "spw='11' fieldcnt=True mode='summary' name='AntSpw011",
        "spw='12' fieldcnt=True mode='summary' name='AntSpw012",
        "spw='13' fieldcnt=True mode='summary' name='AntSpw013",
        "spw='14' fieldcnt=True mode='summary' name='AntSpw014",
        "spw='15' fieldcnt=True mode='summary' name='AntSpw015",
    ],
    flagbackup=False,
)

np.save("applycal_summary_detailed.npy", summary_3)
