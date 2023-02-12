#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

statwt.py

Created on: 2023/01/27
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Split fields into new measurement set 
and downweight outliers with CASA's statwt task.

"""

import os
import yaml
import shutil
import casatasks

# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)

for field in obs["fields"]["all"]:
    # split measurement set
    print(field)

    target_vis = os.path.join(obs["root"], field + ".ms")

    if os.path.exists(target_vis):
        shutil.rmtree(target_vis)
        shutil.rmtree(target_vis + ".flagversions")

    print(f"splitting")
    casatasks.split(
        obs["ms hanning"],
        outputvis=target_vis,
        datacolumn="corrected",
        field=field,
        correlation="RR,LL",
    )

    # downweight outliers
    print(f"statwt")
    casatasks.statwt(target_vis, datacolumn="data")
