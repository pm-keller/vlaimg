#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

detflags.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Apply VLA deterministic flags. These include antennas not on source,
shadowed antennas, scans with bad intents, autocorrelations, edge channels of spectral windows,
edge channels of the baseband, clipping absolute zero values produced by the correlator and the
first few integrations of a scan (quacking).

"""

import yaml
import casatasks
import numpy as np

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

casatasks.flagmanager(
    obs["ms hanning"], mode="restore", versionname="After Manual Flagging"
)

print(f"\nwriting summary to: detflag_summary_before.npy")
summary = casatasks.flagdata(obs["ms hanning"], mode="summary")
np.save("detflag_summary_before.npy", summary)

print("\napplying online flags")
casatasks.flagcmd(
    obs["ms hanning"],
    inpmode="table",
    reason="any",
    action="apply",
    flagbackup=False,
    useapplied=True,
    plotfile="plot/flaggingreason_vs_time.png",
)

print("\nclipping zeros")
casatasks.flagdata(
    obs["ms hanning"],
    mode="clip",
    clipzeros=True,
    flagbackup=False,
    reason="Zero Clipping",
)

print("\nflagging shadowed antennas")
casatasks.flagdata(obs["ms hanning"], mode="shadow", tolerance=0.0, flagbackup=False)

print("\nflagging scan boundaries")
casatasks.flagdata(
    obs["ms hanning"],
    mode="quack",
    quackinterval=obs["flag data"]["quack interval"],
    quackmode="beg",
    flagbackup=False,
    reason="Quacking",
)

print("\nflagging apriori bad antennas")
casatasks.flagdata(
    obs["ms hanning"],
    mode="manual",
    antenna=obs["flag data"]["bad antennas"],
    flagbackup=False,
    reason="Bad Antennas (obs report)",
)

print("\nflagging apriori bad spectral windows")
casatasks.flagdata(
    obs["ms hanning"],
    mode="manual",
    spw=obs["flag data"]["bad spw"],
    flagbackup=False,
    reason="Bad SPW",
)

print("\nflagging edges of spectral windows")
nchan = obs["flag data"]["spw edge chan"]
for spw in range(16):
    casatasks.flagdata(
        obs["ms hanning"],
        mode="manual",
        spw=f"{spw}:0~{nchan-1};{64-nchan}~63",
        reason="Flag SPW Edge Channels",
    )

print("\nflagging edges of baseband")
nchan = obs["flag data"]["baseband edge chan"]
casatasks.flagdata(
    obs["ms hanning"],
    mode="manual",
    spw=f"0:0~{nchan-1}",
    reason="Flag Baseband Lower Edge Channels",
)
casatasks.flagdata(
    obs["ms hanning"],
    mode="manual",
    spw=f"15:{64-nchan}~63",
    reason="Flag Baseband Upper Edge Channels",
)

print(f"\nwriting summary to: detflag_summary_after.npy\n")
summary = casatasks.flagdata(obs["ms hanning"], mode="summary")
np.save("detflag_summary_after.npy", summary)
