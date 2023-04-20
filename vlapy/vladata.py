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

import os
import numpy as np
from pyuvdata import UVData

from astropy.time import Time

try:
    import casatasks
    import casatools
except:
    pass


def get_uvdata(fpath, data_column="DATA", **kwargs):
    """
    Read uvh5 file and return UVData object.
    """

    uvd = UVData()
    uvd.read(fpath, data_column=data_column, **kwargs)

    return uvd


def get_data_array(uvd, data_column="DATA", **kwargs):
    """
    Get data array with axes (polarisation, baselines, time, frequency) from UVData object.
    """

    if type(uvd) == str:
        uvd = get_uvdata(uvd, data_column=data_column, **kwargs)

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


def makedir(root, name):
    """
    Makes some directories to store the outputs of the processing pipeline
    """

    for folder in ["output", "input", "plots", "caltables"]:
        path = os.path.join(root, name, folder)

        if not os.path.exists(path):
            os.makedirs(path)

    for folder in ["obsplots", "calplots", "dataplots"]:
        path = os.path.join(root, name, "plots", folder)

        if not os.path.exists(path):
            os.makedirs(path)


def casa_times_to_astropy(times):
    """
    Convert CASA time format to astropy Time object
    """

    times = np.atleast_1d(times)

    for i, time in enumerate(times):
        edit = list(time)
        edit[10] = "T"
        time = "".join(edit)
        times[i] = time.replace("/", "-")

    return Time(times, format="isot", scale="utc")


def get_obs_times(ms):
    """
    Get time range of an observation
    """

    msmd = casatools.msmetadata()
    msmd.open(ms)
    timerange = msmd.timerangeforobs(0)

    begin = timerange["begin"]["m0"]["value"]
    end = timerange["end"]["m0"]["value"]

    return Time([begin, end], format="mjd", scale="utc")


def get_ntimes(ms):
    """Get the number of time integrations per scans.

    Parameters
    ----------
    ms : str
        path to measurement set

    Returns
    -------
    list
        number of time integrations per scans.
    """

    root = os.path.dirname(ms)

    msmd = casatools.msmetadata()
    msmd.open(ms)
    nscans = msmd.nscans()
    scans = 1 + np.arange(nscans)

    ntimes = []

    for scan in scans:
        ntimes.append(np.shape(msmd.timesforscan(scan))[0])

    printfile = os.path.join(root, "output/ntimes.txt")
    print(f"\nsaving number of time integrations per scan: {printfile}")
    np.savetxt(printfile, ntimes)

    return ntimes


def get_field_names(ms):
    """Get field names

    Parameters
    ----------
    ms : str
        path to measurement set

    Returns
    -------
    dict
        field name strings of flux calibrators, phase calibrators, all calibrators, targets and model.
    """

    root = os.path.dirname(ms)

    # get field names
    msmd = casatools.msmetadata()
    msmd.open(ms)
    fluxcal_list = msmd.fieldsforintent("CALIBRATE_FLUX#UNSPECIFIED")
    phasecal_list = msmd.fieldsforintent("CALIBRATE_PHASE#UNSPECIFIED")
    target_list = msmd.fieldsforintent("OBSERVE_TARGET#UNSPECIFIED")
    calibrators_list = np.hstack([fluxcal_list, phasecal_list])
    fieldnames = msmd.fieldnames 

    # get model name
    model = msmd.namesforfields(fluxcal_list[0])[0]
    if model == "J0542+4951":
        model = "3C147"
    model += "_L.im"

    fluxcal, phasecal, calibrators, targets = "", "", "", ""

    # make field name strings
    for field in fluxcal_list:
        fluxcal += msmd.namesforfields(field)[0] + ","
    for field in phasecal_list:
        phasecal += msmd.namesforfields(field)[0] + ","
    for field in calibrators_list:
        calibrators += msmd.namesforfields(field)[0] + ","
    for field in target_list:
        targets += msmd.namesforfields(field)[0] + ","

    # remove last comma
    fluxcal = fluxcal[:-1]
    phasecal = phasecal[:-1]
    calibrators = calibrators[:-1]
    targets = targets[:-1]

    return {
        "names": fieldnames,
        "fluxcal": fluxcal,
        "phasecal": phasecal,
        "calibrators": calibrators,
        "targets": targets,
        "model": model,
    }


def get_versionnames(ms):
    """Get a list of flagversions

    Parameters
    ----------
    ms : str
        path to measurement set

    Returns
    -------
    list of str
        flag version names
    """

    if os.path.exists(ms + ".flagversions"):
        return [vname.split(".")[-1] for vname in os.listdir(ms + ".flagversions")]
    else:
        return []
