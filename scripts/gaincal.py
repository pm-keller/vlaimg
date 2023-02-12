#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

gaincal.py

Created on: 2023/02/07
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Final round of gain calibration

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

print("\nGAIN CALIBRATION")

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

# remove calibration files
for filename in glob.glob("*.last"):
    if os.path.exists(filename):
        os.remove(filename)

print("\nsplit calibrators to calibrators.ms")
if os.path.exists("calibrators.ms"):
    shutil.rmtree("calibrators.ms")

casatasks.split(
    obs["ms hanning"],
    outputvis="calibrators.ms",
    field=obs["fields"]["calibrators"],
    datacolumn="corrected",
    keepflags=False,
)

print("\nflagging summary before")
summary = casatasks.flagdata(
    "calibrators.ms",
    mode="summary",
    field=obs["fields"]["calibrators"],
    flagbackup=False,
)
np.save("gaincal_summary_before.npy", summary)

for i in range(2):
    gaincal_table = f"{obs['name']}.G{3+i}"
    if os.path.exists(gaincal_table):
        shutil.rmtree(gaincal_table)

    for j, calibrator in enumerate(obs["fields"]["calibrators"].split(",")):
        print(f"\n{calibrator} gain calibration")

        if j == 0:
            append = False
        else:
            append = True

        casatasks.gaincal(
            "calibrators.ms",
            caltable=gaincal_table,
            field=calibrator,
            refant=obs["refant"],
            solint="int",
            combine="scan",
            gaintype="G",
            calmode="ap",
            minsnr=5.0,
            parang=True,
            append=append,
        )

    print("plot gains")
    for ant in range(obs["antennas"]):
        # amplitude
        casaplotms.plotms(
            gaincal_table,
            xaxis="time",
            yaxis="amp",
            coloraxis="spw",
            iteraxis="corr",
            gridcols=2,
            antenna=str(ant),
            highres=True,
            overwrite=True,
            plotfile=f"plots/{gaincal_table}_amp_ant_{ant}.png",
        )
        # phase
        casaplotms.plotms(
            gaincal_table,
            xaxis="time",
            yaxis="phase",
            coloraxis="spw",
            iteraxis="corr",
            gridcols=2,
            antenna=str(ant),
            highres=True,
            overwrite=True,
            plotfile=f"plots/{gaincal_table}_phase_ant_{ant}.png",
        )
