#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

manflags.py

Created on: 2023/01/20
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Apply manual flags to Measurement Set. 
The manual flags are specified in a yaml file "flagging.yaml"

"""

import casatasks
from itertools import product
import yaml

import numpy as np


# star time (h, m, s)
start_time = (12, 17, 0)

with open("flagging.yaml", "r") as file:
    param = yaml.safe_load(file)

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

# flag backup
print("save flags")
casatasks.flagmanager(
    obs["ms hanning"], mode="save", versionname="Before Manual Flagging"
)

print(f"writing summary to: manflag_summary_before.npy\n")
summary = casatasks.flagdata(obs["ms hanning"], mode="summary")
np.save("manflag_summary_before.npy", summary)


for flag in param:
    print(flag)

    spw_str = ""
    ant_str = ""
    time_str = ""
    corr_str = ""

    # flag all baselines formed by antennas in this list
    if "baseline antennas" in param[flag]:
        ants = param[flag]["baseline antennas"]

        # all possible baseline combinations
        antpairs = list(product(ants, ants))

        # make CASA antenna string
        for ant1, ant2 in antpairs:
            ant_str += f"{ant1}&{ant2};"

        ant_str = ant_str[:-1]

    # flag antennas
    if "antennas" in param[flag]:
        ants = param[flag]["antennas"]

        for ant in ants:
            ant_str += f"{ant};"

        ant_str = ant_str[:-1]

    # flag correlation
    if "correlation" in param[flag]:
        corrs = param[flag]["antennas"]

        for corr in corrs:
            corr_str += f"{corr};"

        corr_str = corr_str[:-1]

    # flag time ranges
    if "times" in param[flag]:
        time_str = param[flag]["times"]

    # flag spectral windows
    if "spw" in param[flag]:
        spw_str = param[flag]["spw"]

    casatasks.flagdata(
        obs["ms hanning"],
        mode="manual",
        spw=spw_str,
        antenna=ant_str,
        timerange=time_str,
        correlation=corr_str,
        reason=param[flag]["reason"],
    )

casatasks.flagmanager(
    obs["ms hanning"], mode="save", versionname="After Manual Flagging"
)

print(f"writing summary to: manflag_summary_after.npy\n")
summary = casatasks.flagdata(obs["ms hanning"], mode="summary")
np.save("manflag_summary_after.npy", summary)
