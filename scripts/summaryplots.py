#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

summaryplots.py

Created on: 2023/02/09
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Make summary plots of the calibrated measurement set.

"""

import yaml
import casaplotms
import numpy as np

# set display environment variable
from pyvirtualdisplay import Display

display = Display(visible=0, size=(2048, 2048))
display.start()


# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

print("\nMAKE SUMMARY PLOTS\n")
for field in obs["fields"]["all"]:
    print(field)

    casaplotms.plotms(
        obs["ms hanning"],
        xaxis="uvwave",
        yaxis="amp",
        ydatacolumn="corrected",
        field=field,
        coloraxis="spw",
        highres=True,
        overwrite=True,
        plotfile=f"plots/{field}_amp_vs_uvdist_corrected.png",
    )
    casaplotms.plotms(
        obs["ms hanning"],
        xaxis="freq",
        yaxis="amp",
        ydatacolumn="corrected",
        field=field,
        coloraxis="antenna1",
        highres=True,
        overwrite=True,
        plotfile=f"plots/{field}_amp_vs_freq_corrected.png",
    )
    casaplotms.plotms(
        obs["ms hanning"],
        xaxis="freq",
        yaxis="phase",
        ydatacolumn="corrected",
        field=field,
        coloraxis="antenna1",
        highres=True,
        overwrite=True,
        plotfile=f"plots/{field}_phase_vs_freq_corrected.png",
    )
