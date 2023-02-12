#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

semifinalcal.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Semi-final delay and bandpass calibration.

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


print("\nCALIBRATION ROUND 2")

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

# specify gain tables
gaintable = []
for table in ["opac", "rq", "antpos"]:
    gaintable.append(f"{obs['name']}.{table}")

# specify calibration table names
delay_init_table = f"{obs['name']}.p.G1"
delay_table = f"{obs['name']}.K1"
bandpass_init_table = f"{obs['name']}.ap.G1"
bandpass_table = f"{obs['name']}.B1"

# spetral window ranges for initial calibration. Generously avoid the edges of spectral windows.
spw_str = ""
for spw in range(obs["spw"]):
    spw_str += f"{spw}:21~43,"
spw_str = spw_str[:-1]

# remove calibration files
for filename in glob.glob("*.last"):
    os.remove(filename)

for table in [delay_init_table, delay_table, bandpass_init_table, bandpass_table]:
    if os.path.exists(table):
        shutil.rmtree(table)

print("\ndelay initial phase calibration")
casatasks.gaincal(
    obs["ms hanning"],
    field=obs["fields"]["fluxcal"],
    refant=obs["refant"],
    spw=spw_str,
    gaintype="G",
    calmode="p",
    solint="int",
    caltable=delay_init_table,
    gaintable=gaintable,
    parang=True,
)
gaintable.append(delay_init_table)

print("plot delay initial phase calibration")
for ant in range(obs["antennas"]):
    casaplotms.plotms(
        delay_init_table,
        field=obs["fields"]["fluxcal"],
        xaxis="time",
        yaxis="phase",
        coloraxis="spw",
        iteraxis="corr",
        gridcols=2,
        plotrange=[-1, -1, -180, 180],
        antenna=str(ant),
        highres=True,
        overwrite=True,
        plotfile=f"plots/delay_init_cal_ant_{ant}_sf.png",
    )

print("\ndelay calibration")
casatasks.gaincal(
    obs["ms hanning"],
    field=obs["fields"]["fluxcal"],
    refant=obs["refant"],
    gaintype="K",
    calmode="p",
    solint="inf",
    caltable=delay_table,
    gaintable=gaintable,
    parang=True,
)

gaintable.remove(delay_init_table)
gaintable.append(delay_table)

print("plot delay phase calibration")
casaplotms.plotms(
    delay_table,
    field=obs["fields"]["fluxcal"],
    xaxis="antenna1",
    yaxis="delay",
    coloraxis="baseline",
    iteraxis="corr",
    gridcols=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/delay_cal_sf.png",
)

print("\nbandpass initial gain calibration")
casatasks.gaincal(
    obs["ms hanning"],
    field=obs["fields"]["fluxcal"],
    refant=obs["refant"],
    spw=spw_str,
    gaintype="G",
    calmode="ap",
    solint="int",
    minsnr=5.0,
    caltable=bandpass_init_table,
    gaintable=gaintable,
    parang=True,
)
gaintable.append(bandpass_init_table)

print("plot bandpass initial calibration")
for ant in range(obs["antennas"]):
    # phase
    casaplotms.plotms(
        vis=bandpass_init_table,
        field=obs["fields"]["fluxcal"],
        xaxis="time",
        yaxis="phase",
        coloraxis="spw",
        iteraxis="corr",
        gridcols=2,
        plotrange=[-1, -1, -180, 180],
        antenna=str(ant),
        highres=True,
        overwrite=True,
        plotfile=f"plots/bandpass_init_cal_phase_ant_{ant}_sf.png",
    )
    # amplitude
    casaplotms.plotms(
        vis=bandpass_init_table,
        field=obs["fields"]["fluxcal"],
        xaxis="time",
        yaxis="amp",
        coloraxis="spw",
        iteraxis="corr",
        gridcols=2,
        antenna=str(ant),
        highres=True,
        overwrite=True,
        plotfile=f"plots/bandpass_init_cal_amp_ant_{ant}_sf.png",
    )

print("\nbandpass calibration")
casatasks.bandpass(
    obs["ms hanning"],
    field=obs["fields"]["fluxcal"],
    refant=obs["refant"],
    solint="inf",
    minsnr=5.0,
    caltable=bandpass_table,
    gaintable=gaintable,
    parang=True,
)
gaintable.append(bandpass_table)

print("plot bandpass calibration")
for ant in range(obs["antennas"]):
    # phase
    casaplotms.plotms(
        vis=bandpass_table,
        field=obs["fields"]["fluxcal"],
        xaxis="freq",
        yaxis="phase",
        coloraxis="spw",
        iteraxis="corr",
        gridcols=2,
        antenna=str(ant),
        highres=True,
        overwrite=True,
        plotfile=f"plots/bandpass_cal_phase_ant_{ant}_sf.png",
    )
    # amplitude
    casaplotms.plotms(
        vis=bandpass_table,
        field=obs["fields"]["fluxcal"],
        xaxis="freq",
        yaxis="amp",
        coloraxis="spw",
        iteraxis="corr",
        gridcols=2,
        antenna=str(ant),
        highres=True,
        overwrite=True,
        plotfile=f"plots/bandpass_cal_amp_ant_{ant}_sf.png",
    )

print("\nflag bandpass outliers")
casatasks.flagdata(
    bandpass_table,
    mode="clip",
    correlation="ABS_ALL",
    clipminmax=[0.0, 2.0],
    datacolumn="CPARAM",
    action="apply",
    flagbackup=False,
)

print("\napply calibration")
casatasks.applycal(
    obs["ms hanning"],
    gaintable=gaintable,
    interp=["", "", "", "", "", "nearest,nearestflag"],
    calwt=False,
    parang=True,
    applymode="calflagstrict",
)

print("\nplot corrected data")
casaplotms.plotms(
    vis=obs["ms hanning"],
    field=obs["fields"]["fluxcal"],
    xaxis="freq",
    yaxis="amp",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    gridrows=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/fluxcal_init_cal_amp_vs_freq_sf.png",
)

casaplotms.plotms(
    vis=obs["ms hanning"],
    field=obs["fields"]["fluxcal"],
    xaxis="freq",
    yaxis="phase",
    coloraxis="antenna1",
    iteraxis="corr",
    ydatacolumn="corrected",
    gridcols=2,
    gridrows=2,
    highres=True,
    overwrite=True,
    plotfile=f"plots/fluxcal_init_cal_phase_vs_freq_sf.png",
)
