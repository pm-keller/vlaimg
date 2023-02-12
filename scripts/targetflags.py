#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

targetflags.py

Created on: 2023/02/06
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Flaggin on targets.

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

casatasks.flagmanager(
    obs["ms hanning"], mode="restore", versionname="before target flagging"
)

print("\nflagging summary before")
summary_1 = casatasks.flagdata(
    obs["ms hanning"],
    mode="summary",
    field=obs["fields"]["targets"],
    name="before target flagging",
)
np.save("target_flags_summary_before.npy", summary_1)

print(
    f"\nsplit time-averaged targets fields to: {obs['ms hanning']+'before_targets_flagging.targets.averaged'}"
)

if os.path.exists(obs["ms hanning"] + "before_targets_flagging.targets.averaged"):
    shutil.rmtree(obs["ms hanning"] + "before_targets_flagging.targets.averaged")

casatasks.mstransform(
    obs["ms hanning"],
    outputvis=obs["ms hanning"] + "before_targets_flagging.targets.averaged",
    field=obs["fields"]["targets"],
    correlation="RR,LL",
    datacolumn="corrected",
    keepflags=False,
    timeaverage=True,
    timebin="1e8",
    timespan="scan",
    reindex=False,
)

print(f"\nsave flag version: 'before target flagging'")
casatasks.flagmanager(
    obs["ms hanning"],
    mode="save",
    versionname="before target flagging",
    comment="flagversion before target flagging",
    merge="replace",
)

print(f"\nCompute noise thresholds for ABS_RL correlation")
thresholds = casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_RL",
    datacolumn="corrected",
    timedevscale=5.0,
    freqdevscale=5.0,
    action="calculate",
    extendflags=False,
    flagbackup=False,
)

print(f"\nRFlag on ABS_RL with timedevscale=5.0 and freqdevscale=5.0")
casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_RL",
    datacolumn="corrected",
    timedev=thresholds["report0"]["timedev"],
    freqdev=thresholds["report0"]["freqdev"],
    timedevscale=5.0,
    freqdevscale=5.0,
    action="apply",
    extendflags=False,
    flagbackup=False,
)

print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nCompute noise thresholds for ABS_LR correlation")
thresholds = casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_LR",
    datacolumn="corrected",
    timedevscale=5.0,
    freqdevscale=5.0,
    action="calculate",
    extendflags=False,
    flagbackup=False,
)

print(f"\nRFlag on ABS_LR with timedevscale=5.0 and freqdevscale=5.0")
casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_LR",
    datacolumn="corrected",
    timedev=thresholds["report0"]["timedev"],
    freqdev=thresholds["report0"]["freqdev"],
    timedevscale=5.0,
    freqdevscale=5.0,
    action="apply",
    extendflags=False,
    flagbackup=False,
)

print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nCompute noise thresholds for ABS_LL correlation")
thresholds = casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_LL",
    datacolumn="corrected",
    timedevscale=5.0,
    freqdevscale=5.0,
    action="calculate",
    extendflags=False,
    flagbackup=False,
)

print(f"\nRFlag on ABS_LL with timedevscale=5.0 and freqdevscale=5.0")
casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_LL",
    datacolumn="corrected",
    timedev=thresholds["report0"]["timedev"],
    freqdev=thresholds["report0"]["freqdev"],
    timedevscale=5.0,
    freqdevscale=5.0,
    action="apply",
    extendflags=False,
    flagbackup=False,
)

print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nCompute noise thresholds for ABS_RR correlation")
thresholds = casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_RR",
    datacolumn="corrected",
    timedevscale=5.0,
    freqdevscale=5.0,
    action="calculate",
    extendflags=False,
    flagbackup=False,
)

print(f"\nRFlag on ABS_RR with timedevscale=5.0 and freqdevscale=5.0")
casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["targets"],
    correlation="ABS_RR",
    datacolumn="corrected",
    timedev=thresholds["report0"]["timedev"],
    freqdev=thresholds["report0"]["freqdev"],
    timedevscale=5.0,
    freqdevscale=5.0,
    action="apply",
    extendflags=False,
    flagbackup=False,
)

print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_LR with timecutoff=3, freqcutoff=3, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["targets"],
    correlation="ABS_LR",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=3.0,
    freqcutoff=3.0,
    freqfit="line",
    flagdimension="freq",
    action="apply",
    extendflags=False,
    flagbackup=False,
)


print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_RL with timecutoff=3, freqcutoff=3, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["targets"],
    correlation="ABS_RL",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=3.0,
    freqcutoff=3.0,
    freqfit="line",
    flagdimension="freq",
    action="apply",
    extendflags=False,
    flagbackup=False,
)

print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_LL with timecutoff=3, freqcutoff=3, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["targets"],
    correlation="ABS_LL",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=3.0,
    freqcutoff=3.0,
    freqfit="line",
    flagdimension="freq",
    action="apply",
    extendflags=False,
    flagbackup=False,
)

print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_RR with timecutoff=3, freqcutoff=3, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["targets"],
    correlation="ABS_RR",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=3.0,
    freqcutoff=3.0,
    freqfit="line",
    flagdimension="freq",
    action="apply",
    extendflags=False,
    flagbackup=False,
)

print(f"\nextend flags across polarisations")
casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

casatasks.flagdata(
    obs["ms hanning"],
    mode="extend",
    field=obs["fields"]["targets"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    growaround=True,
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print("\nfinal round of RFLag with timedevscale=4.0, freqdevscale=4.0")
casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    correlation="ABS_RR,LL",
    datacolumn="corrected",
    ntime="scan",
    timedevscale=4.0,
    freqdevscale=4.0,
    action="apply",
    extendflags=False,
    flagbackup=True,
)

print("\nflagging summary after")
summary_2 = casatasks.flagdata(
    obs["ms hanning"],
    mode="summary",
    field=obs["fields"]["targets"],
    name="after target flagging",
)
np.save("target_flags_summary_after.npy", summary_2)

print(
    f"\nsplit time-averaged targets fields to: {obs['ms hanning']+'after_targets_flagging.targets.averaged'}"
)
if os.path.exists(obs["ms hanning"] + "after_targets_flagging.targets.averaged"):
    shutil.rmtree(obs["ms hanning"] + "after_targets_flagging.targets.averaged")

casatasks.mstransform(
    obs["ms hanning"],
    outputvis=obs["ms hanning"] + "after_targets_flagging.targets.averaged",
    field=obs["fields"]["targets"],
    correlation="RR,LL",
    datacolumn="corrected",
    keepflags=False,
    timeaverage=True,
    timebin="1e8",
    timespan="scan",
    reindex=False,
)

print("\nplot data before and after flagging")
casaplotms.plotms(
    obs["ms hanning"] + "before_targets_flagging.targets.averaged",
    field=obs["fields"]["targets"],
    xaxis="freq",
    yaxis="amp",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/before_target_flagging_amp_vs_freq.png",
)

casaplotms.plotms(
    obs["ms hanning"] + "after_targets_flagging.targets.averaged",
    field=obs["fields"]["targets"],
    xaxis="freq",
    yaxis="amp",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/after_target_flagging_amp_vs_freq.png",
)

casaplotms.plotms(
    obs["ms hanning"] + "before_targets_flagging.targets.averaged",
    field=obs["fields"]["targets"],
    xaxis="freq",
    yaxis="phase",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/before_target_flagging_phase_vs_freq.png",
)

casaplotms.plotms(
    obs["ms hanning"] + "after_targets_flagging.targets.averaged",
    field=obs["fields"]["targets"],
    xaxis="freq",
    yaxis="phase",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/after_target_flagging_phase_vs_freq.png",
)
