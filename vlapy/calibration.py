#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

calibration.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: VLA calibration routines

"""

import os
import glob
import shutil
import yaml
import casatools
import casatasks
import casaplotms
import numpy as np

from prefect import task
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash)
def setjy(ms):
    """Calculate and set a calibrator model.

    Parameters
    ----------
    ms : str
        path to measurement set

    Returns
    -------
    list of str
        summary file

    """

    msmd = casatools.msmetadata()
    msmd.open(ms)
    field = msmd.fieldsforintent("CALIBRATE_FLUX#UNSPECIFIED")[0]
    model = msmd.namesforfields(field)[0]

    if model == "J0542+4951":
        model = "3C147"

    model += "_L.im"

    print(f"\ncalculating and setting calibration model {model}\n")
    setjy = casatasks.setjy(ms, model=model, field=str(field), usescratch=True)

    root = os.path.dirname(ms)
    printfile = os.path.join(root, "output/setjy.npy")

    print(f"saving summary: {printfile}")
    np.save(printfile, setjy)

    return printfile


@task(cache_key_fn=task_input_hash)
def priorcal(ms, name):
    """Generate prior calibration tables. These include gain-elevation dependencies,
    atmospheric opacity corrections, antenna offset corrections, and requantizer (rq) gains.
    These are independent of calibrator observations and use external data instead.

    Parameters
    ----------
    ms : str
        path to measurement set
    name : str
        data name

    Returns
    -------
    list of str
        paths to calibration tables
    """

    root = os.path.dirname(ms)
    plotfile = os.path.join(root, "plots/calplots/weather.png")
    printfile = os.path.join(root, "output/weather.npy")

    print(f"\nplot weather: {plotfile}")

    weather = casatasks.plotweather(
        ms,
        seasonal_weight=0.5,
        doPlot=True,
        plotName=plotfile,
    )

    print(f"\nsaving opacities: {printfile}")
    np.save(printfile, weather)

    print("\nopacity corrections")
    opacity_table = os.path.join(root, f"caltables/{name}.opac")

    casatasks.gencal(
        ms,
        caltable=opacity_table,
        caltype="opac",
        spw="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15",
        parameter=weather,
    )

    print("\nrequantizer gains")
    rq_table = os.path.join(root, f"caltables/{name}.rq")
    casatasks.gencal(ms, caltable=rq_table, caltype="rq")

    print("\nEVLA switched power gains")
    swpow_table = os.path.join(root, f"caltables/{name}.swpow")
    casatasks.gencal(
        ms,
        swpow_table,
        caltype="swpow",
    )

    print("\nantenna position corrections\n")
    antpos_table = os.path.join(root, f"caltables/{name}.antpos")
    casatasks.gencal(ms, caltable=antpos_table, caltype="antpos")

    # ionospheric TEC corrections
    # Currently, this does not work. Error: local variable 'plot_name' referenced before assignment
    # print("ionospheric TEC corrections")
    # tec_image, tec_rms_image = tec_maps.create(vis)
    # tec = f"{name}.tecim"
    # casatasks.gencal(vis, caltable=tec, caltype="tecim", infile=tec_image)

    # Gain table absent
    # print("\ngain curve corrections")
    # gc_table = f"{name}.gc"
    # casatasks.gencal(
    #    ms,
    #    caltable=gc_table,
    #    caltype="gc",
    # )

    return [opacity_table, swpow_table, antpos_table]


@task(cache_key_fn=task_input_hash)
def single_chan_gaincal(ms, name, field, refant, chan, priortables):
    """Initial gain calibration using a single channel.
    This initial calibration is to enable subsequent automated RFI excision.

    Parameters
    ----------
    ms : str
        path to measurement set
    name : str
        data name
    field : str
        field name
    refant : str
        reference antenna
    nspw : int
        number of spectral windows
    priortables : list of str
        prior calibration tables

    Returns
    -------
    list of str
        calibration tables
    """

    # specify gain tables
    gaintables = [].append(priortables)

    # specify calibration table names
    root = os.path.dirname(ms)
    caltable = os.path.join(root, f"caltables/{name}.p.G0")

    if os.path.exists(caltable):
        shutil.rmtree(caltable)

    print("\nsingle channel gain calibration")
    casatasks.gaincal(
        ms,
        field=field,
        refant=refant,
        spw=chan,
        gaintype="G",
        calmode="ap",
        solint="int",
        minsnr=5.0,
        caltable=caltable,
        gaintable=gaintables,
        parang=True,
    )
    gaintables.append(caltable)

    print("\napply calibration")
    casatasks.applycal(
        ms,
        gaintable=gaintables,
        calwt=False,
        parang=True,
        applymode="calflagstrict",
    )

    return gaintables


@task(cache_key_fn=task_input_hash)
def initcal(ms, name, field, refant, calchan, priortables, rnd=0):
    """Initial delay and bandpass calibration.
    This initial calibration is to enable subsequent automated RFI excision.

    Parameters
    ----------
    ms : str
        path to measurement set
    name : str
        data name
    field : str
        field name
    refant : str
        reference antenna
    calchan : str
        calibration channels
    priortables : list of str
        prior calibration tables
    rnd : int
        calibration round

    Returns
    -------
    list of str
        calibration tables
    """

    print(f"\nsave flag version: before_initcal_round_{rnd}")
    versionnames = casatasks.flagmanager(ms, mode="list")
    if f"before_initcal_round_{rnd}" in versionnames:
        casatasks.flagmanager(
            ms,
            mode="restore",
            versionname=f"before_initcal_round_{rnd}",
        )
    else:
        casatasks.flagmanager(ms, mode="save", versionname=f"before_initcal_round_{rnd}")

    # specify gain tables
    gaintables = [].append(priortables)

    # specify calibration table names
    root = os.path.dirname(ms)
    delay_init_table = os.path.join(root, f"caltables/{name}.p.G{rnd}")
    delay_table = os.path.join(root, f"caltables/{name}.K{rnd}")
    bandpass_init_table = os.path.join(root, f"caltables/{name}.ap.G{rnd}")
    bandpass_table = os.path.join(root, f"caltables/{name}.B{rnd}")

    # remove calibration files
    for filename in [
        delay_init_table,
        delay_table,
        bandpass_init_table,
        bandpass_table,
    ]:
        if os.path.exists(filename):
            shutil.rmtree(filename)

    print("\ndelay initial phase calibration")
    casatasks.gaincal(
        ms,
        field=field,
        refant=refant,
        spw=calchan,
        gaintype="G",
        calmode="p",
        solint="int",
        minsnr=5.0,
        caltable=delay_init_table,
        gaintable=gaintables,
        parang=True,
    )
    gaintables.append(delay_init_table)

    print("\ndelay calibration")
    casatasks.gaincal(
        ms,
        field=field,
        refant=refant,
        gaintype="K",
        calmode="p",
        solint="inf",
        minsnr=5.0,
        caltable=delay_table,
        gaintable=gaintables,
        parang=True,
    )

    gaintables.remove(delay_init_table)
    gaintables.append(delay_table)

    print("\nbandpass initial gain calibration")
    casatasks.gaincal(
        ms,
        field=field,
        refant=refant,
        spw=calchan,
        gaintype="G",
        calmode="ap",
        solint="int",
        minsnr=5.0,
        caltable=bandpass_init_table,
        gaintable=gaintables,
        parang=True,
    )
    gaintables.append(bandpass_init_table)

    print("\nbandpass calibration")
    casatasks.bandpass(
        ms,
        field=field,
        refant=refant,
        solint="inf",
        minsnr=5.0,
        caltable=bandpass_table,
        gaintable=gaintables,
        parang=True,
    )
    gaintables.append(bandpass_table)

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
        ms,
        gaintable=gaintables,
        interp=["", "", "", "", "", "nearest,nearestflag"],
        calwt=False,
        parang=True,
        applymode="calflagstrict",
        flagbackup=False
    )

    if f"after_initcal_round_{rnd}" in versionnames:
        casatasks.flagmanager(
            ms,
            mode="delete",
            versionname=f"after_initcal_round_{rnd}",
        )
    casatasks.flagmanager(ms, mode="save", versionname=f"after_initcal_round_{rnd}")

    return gaintables
