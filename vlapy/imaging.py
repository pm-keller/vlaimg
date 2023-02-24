#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

imaging.py

Created on: 2023/02/22
Author: Pascal M. Keller
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: modules for imaging VLA data with CASA

"""


import os
import shutil
import casatasks
import casatools


def prep(ms, overwrite=False):
    """Prepare measurement sets for imaging

    Parameters
    ----------
    ms : str
        path to measurement set
    overwrite : bool, optional
        if true, overwrite existing data, by default False
    """

    root = os.path.dirname(ms)

    msmd = casatools.msmetadata()
    msmd.open(ms)

    fields = msmd.fieldnames()

    for field in fields:
        # split measurement set
        print(field)

        target_vis = os.path.join(root, field + ".ms")

        if os.path.exists(target_vis) and overwrite:
            shutil.rmtree(target_vis)
            shutil.rmtree(target_vis + ".flagversions")

        if not os.path.exists(target_vis):
            print(f"splitting")
            casatasks.split(
                ms,
                outputvis=target_vis,
                datacolumn="corrected",
                field=field,
                correlation="RR,LL",
            )

        # downweight outliers
        print(f"statwt")
        casatasks.statwt(target_vis, datacolumn="data")
