#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

priorcal.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Generate prior calibration tables. These include gain-elevation dependencies,
atmospheric opacity corrections, antenna offset corrections, and requantizer (rq) gains. 
These are independent of calibrator observations and use external data instead.

"""

import yaml
import numpy as np
import casatasks

# from casatasks.private import tec_maps

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

print("\nplot weather")
weather = casatasks.plotweather(
    obs["ms hanning"],
    seasonal_weight=0.5,
    doPlot=True,
    plotName="plots/weather.png",
)
np.save("weather.npy", weather)

print("\nopacity corrections")
opacity_table = f"{obs['name']}.opac"
opacity = casatasks.gencal(
    obs["ms hanning"],
    caltable=opacity_table,
    caltype="opac",
    spw="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15",
    parameter=weather,
)

print("\nrequantizer gains")
rq_table = f"{obs['name']}.rq"
rq = casatasks.gencal(obs["ms hanning"], caltable=rq_table, caltype="rq")

print("\nEVLA switched power gains")
swpow_table = f"{obs['name']}.swpow"
swpow = casatasks.gencal(
    obs["ms hanning"],
    swpow_table,
    caltype="swpow",
)

print("\nantenna position corrections\n")
antpos_table = f"{obs['name']}.antpos"
antpos = casatasks.gencal(obs["ms hanning"], caltable=antpos_table, caltype="antpos")

# ionospheric TEC corrections
# Currently, this does not work. Error: local variable 'plot_name' referenced before assignment
# print("ionospheric TEC corrections")
# tec_image, tec_rms_image = tec_maps.create(vis)
# tec = f"{name}.tecim"
# casatasks.gencal(vis, caltable=tec, caltype="tecim", infile=tec_image)

# Gain table absent
# print("\ngain curve corrections")
# gc_table = f"{obs['name']}.gc"
# casatasks.gencal(
#    obs["ms hanning"],
#    caltable=gc_table,
#    caltype="gc",
# )
