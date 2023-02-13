#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

hanning.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Apply hanning smoothing to data. 
This mitigates the Gibbs phenomenon around strong RFI spikes
and reduces the spectral resolution and thereby the data volume
by a factor of two. 

"""

import os
import shutil
import yaml
import casatasks
import casaplotms
import numpy as np

# set display environment variable
from pyvirtualdisplay import Display

display = Display(visible=0, size=(2048, 2048))
display.start()


# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

# hanning smoothing
print("\nhanning smooth\n")
print(f"input MS: {obs['ms']}")
print(f"outpu MS: {obs['ms hanning']}")

# plot array layout
casatasks.plotants(obs["ms"], figfile="plots/antlayout.png")

# plot elevation vs. time
casaplotms.plotms(
    obs["ms"],
    xaxis="time",
    yaxis="elevation",
    coloraxis="field",
    plotfile="plots/elevation_vs_time.png",
    overwrite=True,
    highres=True,
)

# plot antenna data stream
print("\nplot antenna data stream")
casaplotms.plotms(
    obs["ms hanning"],
    antenna="ea01",
    xaxis="time",
    yaxis="antenna2",
    plotrange=[-1, -1, 0, 26],
    coloraxis="field",
    highres=True,
    overwrite=True,
    plotfile=f"{root}/data_stream.png",
)

# write measurement set metadata to file
print("\nwriting listobs to file: listobs.txt")
casatasks.listobs(obs["ms hanning"], listfile="listobs.txt")
