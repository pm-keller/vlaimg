#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

calibration.py

Created on: 2023/01/24
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Calibrate measurement set using CASA

"""

import os
import shutil
import glob
import numpy as np
import casatasks

vis = "/lustre/aoc/projects/hera/pkeller/data/VLA/19A-056.sb37262953.eb37267948.58744.511782789355/19A-056.sb37262953.eb37267948.58744.511782789355-hanning.ms"

name = "19A-056-1"
fluxcal_field = "J0542+4951"
phasecal_field_1 = "J1033+4116"
phasecal_field_2 = "J1130+3815"
qso_1 = "QSO J1048+4637"
qso_2 = "QSO J1137+3549"
model_im = "3C147_L.im"
refant = "ea19"

antpos = f"{name}.antpos"
rq = f"{name}.rq"
initcal_all = f"{name}.G0all"
initcal = f"{name}.G0"
delaycal = f"{name}.K1"
bandpasscal = f"{name}.B1"
gaincal = f"{name}.G1"
fluxscale = f"{name}.fluxscale"

# remove calibration files
for filename in glob.glob("*.last"):
    os.remove(filename)

for filename in [initcal_all, initcal, delaycal, bandpasscal, gaincal, fluxscale]:
    shutil.rmtree(filename)

# clear calibration
casatasks.clearcal(vis)
gaintable = [antpos, rq]

# initial phase calibration
print("initial phase calibration")
casatasks.gaincal(
    vis,
    caltable=initcal_all,
    field="0,1,3",
    refant=refant,
    gaintype="G",
    calmode="p",
    solint="int",
    minsnr=5,
    gaintable=gaintable,
)

casatasks.gaincal(
    vis,
    caltable=initcal,
    field=fluxcal_field,
    refant=refant,
    gaintype="G",
    calmode="p",
    solint="int",
    minsnr=5,
    gaintable=gaintable,
)

gaintable.append(initcal)
# absolute flux calibration
print("absolute flux calibration")
casatasks.setjy(vis, field=fluxcal_field, model=model_im, scalebychan=True)

# delay calibration
print("delay calibration")
casatasks.gaincal(
    vis,
    caltable=delaycal,
    field=fluxcal_field,
    solint="inf",
    refant=refant,
    gaintype="K",
    gaintable=gaintable,
    parang=True,
)
gaintable.append(delaycal)

# bandpass calibration
print("bandpass calibration")
casatasks.bandpass(
    vis,
    caltable=bandpasscal,
    field=fluxcal_field,
    solint="inf",
    refant=refant,
    minsnr=3.0,
    parang=True,
    gaintable=gaintable,
)
gaintable.append(bandpasscal)


# gain calibration
print("gain calibration ...")
gaintable = [antpos, rq, delaycal, bandpasscal]

# on primary (flux) calibrator
print("... primary calibrator")
casatasks.gaincal(
    vis,
    caltable=gaincal,
    field=fluxcal_field,
    solint="inf",
    refant=refant,
    minsnr=3.0,
    gaintype="G",
    calmode="ap",
    solnorm=False,
    parang=True,
    gaintable=gaintable,
    interp=["", "", "", "nearest"],
)

# on first phase calibrator
print("... phase calibrator 1")
casatasks.gaincal(
    vis,
    caltable=gaincal,
    field=phasecal_field_1,
    solint="inf",
    refant=refant,
    minsnr=3.0,
    gaintype="G",
    calmode="ap",
    solnorm=False,
    parang=True,
    gaintable=gaintable,
    interp=["", "", "", "nearest"],
    append=True,
)

# on second phase calibrator
print("... phase calibrator 2")
casatasks.gaincal(
    vis,
    caltable=gaincal,
    field=phasecal_field_2,
    solint="inf",
    refant=refant,
    minsnr=3.0,
    gaintype="G",
    calmode="ap",
    solnorm=False,
    parang=True,
    gaintable=gaintable,
    interp=["", "", "", "nearest"],
    append=True,
)
gaintable.append(gaincal)

# scaling the amplitude gains
print("scaling the amplitude gains")
myscale = casatasks.fluxscale(
    vis,
    caltable=gaincal,
    fluxtable=fluxscale,
    reference=fluxcal_field,
    transfer=[phasecal_field_1, phasecal_field_2],
    incremental=False,
)
gaintable = [antpos, rq, delaycal, bandpasscal, fluxscale]

# apply the calibration
print("applying calibration to fields:")
for field in [fluxcal_field, phasecal_field_1, phasecal_field_2]:
    print(field)
    casatasks.applycal(
        vis,
        field=field,
        gaintable=gaintable,
        gainfield=["", "", "", "", field],
        interp=["", "", "", "", "nearest"],
    )

# apply calibration to first target
print(qso_1)
casatasks.applycal(
    vis,
    field=qso_1,
    gaintable=gaintable,
    gainfield=["", "", "", "", phasecal_field_1],
    interp=["", "", "", "", "linear"],
)

print(qso_2)
# apply calibration to second target
casatasks.applycal(
    vis,
    field=qso_2,
    gaintable=gaintable,
    gainfield=["", "", "", "", phasecal_field_2],
    interp=["", "", "", "", "linear"],
)
