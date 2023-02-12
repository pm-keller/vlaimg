#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

setjy.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Calculate and set a calibrator model.

"""

import yaml
import casatasks
import casaplotms
import numpy as np

from pyvirtualdisplay import Display

# set display environment variable for plotms
display = Display(visible=0, size=(2048, 2048))
display.start()

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

print(f"\ncalculating and setting calibration model {obs['model']}\n")
setjy = casatasks.setjy(
    obs["ms hanning"], model=obs["model"], field=obs["fields"]["fluxcal"]
)

print("\nplotting model amplitude vs. uv-distance\n")
casaplotms.plotms(
    obs["ms hanning"],
    xaxis="uvdist",
    yaxis="amp",
    coloraxis="spw",
    field=obs["fields"]["fluxcal"],
    ydatacolumn="model",
    plotfile="plots/setjy_model_amp_vs_uvdist.png",
    overwrite=True,
    highres=True,
)

np.save("setjy.npy", setjy)
