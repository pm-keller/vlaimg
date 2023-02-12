#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

vladata.py

Created on: 2023/01/16
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: VLA data oprtations

"""


import numpy as np
from pyuvdata import UVData

try:
    import casatasks
except:
    pass


def get_uvdata(fpath, data_column="DATA"):
    """
    Read uvh5 file and return UVData object.
    """

    uvd = UVData()
    uvd.read(fpath, data_column=data_column)

    return uvd


def get_data_array(uvd, data_column="DATA"):
    """
    Get data array with axes (polarisation, baselines, time, frequency) from UVData object.
    """

    if type(uvd) == str:
        uvd = get_uvdata(uvd, data_column=data_column)

    antpairs = uvd.get_antpairs()
    data = [uvd.get_data(*antpair) for antpair in antpairs]
    data = np.moveaxis(data, -1, 0)

    return data


def get_flag_array(uvd, data_column="DATA"):
    """
    Get data array with axes (polarisation, baselines, time, frequency) from UVData object.
    """

    if type(uvd) == str:
        uvd = get_uvdata(uvd, data_column)

    antpairs = uvd.get_antpairs()
    flags = [uvd.get_flags(*antpair) for antpair in antpairs]
    flags = np.moveaxis(flags, -1, 0)

    return flags


def listobs(ms):
    """
    Print CASA listobs output
    """

    listobs = casatasks.listobs(ms)

    for item in listobs:
        print(item)

        if "field_" in item:
            for subitem in listobs[item]:
                print(subitem, ": ", listobs[item][subitem])
            print("\n")

        elif "scan_" in item:
            for subitem in listobs[item]["0"]:
                print(subitem, ": ", listobs[item]["0"][subitem])
            print("\n")

        else:
            print(listobs[item], "\n")
