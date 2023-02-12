#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

fluxboot.py

Created on: 2023/02/07
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Flux density bootstrapping and spectral index fitting

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

print("\nFLUX BOOTSTRAPPING AND SPECTRAL INDEX FITTING")

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

# specify calibration tables
gaincal_short_table = f"{obs['name']}_short.G"
gaincal_long_table = f"{obs['name']}_long.G"
fluxgaincal_table = f"{obs['name']}_fluxgain.G"

print("\nset model flux density")
casatasks.setjy(
    vis="calibrators.ms",
    field=obs["fields"]["fluxcal"],
    scalebychan=True,
    standard="Perley-Butler 2017",
    model=obs["model"],
    usescratch=True,
)
casatasks.setjy(
    vis=obs["ms hanning"],
    field=obs["fields"]["fluxcal"],
    scalebychan=True,
    standard="Perley-Butler 2017",
    model=obs["model"],
    usescratch=True,
)

print("\nflagging summary before")
summary = casatasks.flagdata(
    vis="calibrators.ms",
    mode="summary",
    field=obs["fields"]["calibrators"],
    flagbackup=False,
)
np.save("fluxboot_summary_before.npy", summary)

# remove calibration files
for filename in [gaincal_long_table, gaincal_short_table, fluxgaincal_table]:
    if os.path.exists(filename):
        shutil.rmtree(filename)

for j, calibrator in enumerate(obs["fields"]["calibrators"].split(",")):
    print(f"\n{calibrator} gain calibration")

    if j == 0:
        append = False
    else:
        append = True

    casatasks.gaincal(
        "calibrators.ms",
        caltable=gaincal_short_table,
        field=calibrator,
        solint="int",
        refant=obs["refant"],
        gaintype="G",
        calmode="p",
        minsnr=3.0,
        parang=True,
        append=append,
    )

    casatasks.gaincal(
        vis="calibrators.ms",
        caltable=gaincal_long_table,
        field=calibrator,
        solint=obs["solint max"],
        refant=obs["refant"],
        minsnr=5.0,
        solnorm=True,
        gaintype="G",
        calmode="ap",
        append=append,
        gaintable=[gaincal_short_table],
    )

print("\nflag gains")
casatasks.flagdata(
    gaincal_long_table,
    field=calibrator,
    mode="clip",
    correlation="ABS_ALL",
    clipminmax=[0.9, 1.1],
    datacolumn="CPARAM",
    clipoutside=True,
    action="apply",
    flagbackup=False,
)

print("\napply calibration")
casatasks.applycal(
    "calibrators.ms",
    gaintable=[
        gaincal_long_table,
    ],
    calwt=[False],
    applymode="flagonlystrict",
    flagbackup=True,
)

for j, calibrator in enumerate(obs["fields"]["calibrators"].split(",")):
    print(f"\n{calibrator} gain calibration")

    if j == 0:
        append = False
    else:
        append = True

    casatasks.gaincal(
        vis="calibrators.ms",
        caltable=fluxgaincal_table,
        field=calibrator,
        solint=obs["solint max"],
        refant=obs["refant"],
        minsnr=5.0,
        solnorm=False,
        gaintype="G",
        calmode="ap",
        append=append,
        gaintable=[gaincal_short_table],
        parang=True,
    )

    if j > 0:
        if os.path.exists(f"{obs['name']}.fluxscale{j}"):
            shutil.rmtree(f"{obs['name']}.fluxscale{j}")

        fluxscale = casatasks.fluxscale(
            "calibrators.ms",
            caltable=fluxgaincal_table,
            fluxtable=f"{obs['name']}.fluxscale{j}",
            reference=[obs["fields"]["fluxcal"]],
            transfer=[calibrator],
            fitorder=2,
        )

        id = [key for key in fluxscale.keys()][0]

        casatasks.setjy(
            "calibrators.ms",
            field=calibrator,
            scalebychan=True,
            fluxdensity=fluxscale[id]["fitFluxd"],
            spix=fluxscale[id]["spidx"],
            reffreq=str(fluxscale[id]["fitRefFreq"]) + "Hz",
            usescratch=True,
            standard="manual",
        )

        casatasks.setjy(
            obs["ms hanning"],
            field=calibrator,
            scalebychan=True,
            fluxdensity=fluxscale[id]["fitFluxd"],
            spix=fluxscale[id]["spidx"],
            reffreq=str(fluxscale[id]["fitRefFreq"]) + "Hz",
            usescratch=True,
            standard="manual",
        )

        np.save(f"phase_cal_model_fit_{j}.npy", fluxscale)

print("\nplot model")
casaplotms.plotms(
    obs["ms hanning"],
    xaxis="freq",
    yaxis="amp",
    coloraxis="field",
    ydatacolumn="model",
    highres=True,
    overwrite=True,
    plotfile=f"plots/model_amp_vs_freq.png",
)
casaplotms.plotms(
    obs["ms hanning"],
    xaxis="freq",
    yaxis="phase",
    coloraxis="field",
    ydatacolumn="model",
    highres=True,
    overwrite=True,
    plotfile=f"plots/model_phase_vs_freq.png",
)
