#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

fluxflags.py

Created on: 2023/02/06
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: First round of flagging on primary calibrator.

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


# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

print("\nwriting summary to: fluxcal_flags_summary_before.npy")
summary_1 = casatasks.flagdata(
    obs["ms hanning"],
    mode="summary",
    field=obs["fields"]["fluxcal"],
    name="before fluxcal flagging",
)
np.save("fluxcal_flags_summary_before.npy", summary_1)

print(
    f"\nsplit time-averaged fluxcal fields to: {obs['ms hanning']+'before_fluxcal_flagging.fluxcal.averaged'}"
)
casatasks.mstransform(
    obs["ms hanning"],
    outputvis=obs["ms hanning"] + "before_fluxcal_flagging.fluxcal.averaged",
    field=obs["fields"]["fluxcal"],
    correlation="RR,LL",
    datacolumn="corrected",
    keepflags=False,
    timeaverage=True,
    timebin="1e8",
    timespan="scan",
    reindex=False,
)

print(f"\nsave flag version: 'before fluxcal flagging'")
casatasks.flagmanager(
    obs["ms hanning"],
    mode="save",
    versionname="before fluxcal flagging",
    comment="flagversion before fluxcal flagging",
    merge="replace",
)

print(f"\nCompute noise thresholds for ABS_RL correlation")
thresholds = casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["fluxcal"],
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
    field=obs["fields"]["fluxcal"],
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
    field=obs["fields"]["fluxcal"],
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
    field=obs["fields"]["fluxcal"],
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
    field=obs["fields"]["fluxcal"],
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
    field=obs["fields"]["fluxcal"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nCompute noise thresholds for REAL_RR correlation")
thresholds = casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["fluxcal"],
    correlation="REAL_RR",
    datacolumn="residual",
    timedevscale=5.0,
    freqdevscale=5.0,
    action="calculate",
    extendflags=False,
    flagbackup=False,
)

print(f"\nRFlag on REAL_RR with timedevscale=5.0 and freqdevscale=5.0")
casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["fluxcal"],
    correlation="REAL_RR",
    datacolumn="residual",
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
    field=obs["fields"]["fluxcal"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)


print(f"\nCompute noise thresholds for REAL_LL correlation")
thresholds = casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["fluxcal"],
    correlation="REAL_LL",
    datacolumn="residual",
    timedevscale=5.0,
    freqdevscale=5.0,
    action="calculate",
    extendflags=False,
    flagbackup=False,
)

print(f"\nRFlag on REAL_LL with timedevscale=5.0 and freqdevscale=5.0")
casatasks.flagdata(
    obs["ms hanning"],
    mode="rflag",
    field=obs["fields"]["fluxcal"],
    correlation="REAL_LL",
    datacolumn="residual",
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
    field=obs["fields"]["fluxcal"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_LR with timecutoff=4, freqcutoff=4, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["fluxcal"],
    correlation="ABS_LR",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=4.0,
    freqcutoff=4.0,
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
    field=obs["fields"]["fluxcal"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_RL with timecutoff=4, freqcutoff=4, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["fluxcal"],
    correlation="ABS_RL",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=4.0,
    freqcutoff=4.0,
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
    field=obs["fields"]["fluxcal"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_LL with timecutoff=4, freqcutoff=4, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["fluxcal"],
    correlation="ABS_LL",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=4.0,
    freqcutoff=4.0,
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
    field=obs["fields"]["fluxcal"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print(f"\nTFCrop on ABS_RR with timecutoff=4, freqcutoff=4, ntime=5")
casatasks.flagdata(
    obs["ms hanning"],
    mode="tfcrop",
    field=obs["fields"]["fluxcal"],
    correlation="ABS_RR",
    datacolumn="corrected",
    ntime=5.0,
    timecutoff=4.0,
    freqcutoff=4.0,
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
    field=obs["fields"]["fluxcal"],
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
    field=obs["fields"]["fluxcal"],
    growtime=100.0,
    growfreq=100.0,
    action="apply",
    growaround=True,
    # flagnearfreq=True,
    extendpols=True,
    extendflags=False,
    flagbackup=False,
)

print("\nwriting summary to: fluxcal_flags_summary_after.npy")
summary_2 = casatasks.flagdata(
    obs["ms hanning"],
    mode="summary",
    field=obs["fields"]["fluxcal"],
    name="after fluxcal flagging",
)
np.save("fluxcal_flags_summary_after.npy", summary_2)

print(
    f"\nsplit time-averaged fluxcal fields to: {obs['ms hanning']+'after_fluxcal_flagging.fluxcal.averaged'}"
)
casatasks.mstransform(
    obs["ms hanning"],
    outputvis=obs["ms hanning"] + "after_fluxcal_flagging.fluxcal.averaged",
    field=obs["fields"]["fluxcal"],
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
    obs["ms hanning"] + "before_fluxcal_flagging.fluxcal.averaged",
    field=obs["fields"]["fluxcal"],
    xaxis="freq",
    yaxis="amp",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/before_fluxcal_flagging_amp_vs_freq.png",
)

casaplotms.plotms(
    obs["ms hanning"] + "after_fluxcal_flagging.fluxcal.averaged",
    field=obs["fields"]["fluxcal"],
    xaxis="freq",
    yaxis="amp",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/after_fluxcal_flagging_amp_vs_freq.png",
)

casaplotms.plotms(
    obs["ms hanning"] + "before_fluxcal_flagging.fluxcal.averaged",
    field=obs["fields"]["fluxcal"],
    xaxis="freq",
    yaxis="phase",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/before_fluxcal_flagging_phase_vs_freq.png",
)

casaplotms.plotms(
    obs["ms hanning"] + "after_fluxcal_flagging.fluxcal.averaged",
    field=obs["fields"]["fluxcal"],
    xaxis="freq",
    yaxis="phase",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/after_fluxcal_flagging_phase_vs_freq.png",
)
