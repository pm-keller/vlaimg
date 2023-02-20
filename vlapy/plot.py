#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

plot.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: modules for plotting data and calibration tables.

"""

import os
import shutil
import yaml
import casatasks
import casatools
import casaplotms
import numpy as np

from prefect import task
from prefect.tasks import task_input_hash

# set display environment variable
from pyvirtualdisplay import Display

display = Display(visible=0, size=(2048, 2048))
display.start()


@task(cache_key_fn=task_input_hash)
def plotobs(ms):
    """Make some plots that describe the observation:

    - Elevation vs. Time
    - Array Layout
    - Antenna Data Stream

    Save CASA's listobs output to a text file.

    Parameters
    ----------
    ms : str
        path to measurement set

    Returns
    -------
    list of strings
        file names of plots
    """

    root = os.path.dirname(ms)
    path = os.path.join(root, "plots/obsplots/")

    files = [
        os.path.join(path, fname)
        for fname in ["antlayout.png", "elevation_vs_time.png", "data_stream.png"]
    ]

    plotfile = os.path.join(path, "antlayout.png")
    print(f"\nplot antenna layout: {plotfile}")
    casatasks.plotants(ms, figfile=os.path.join(path, plotfile))

    plotfile = os.path.join(path, "elevation_vs_time.png")
    print(f"\nplot elevation vs. time: {plotfile}")
    casaplotms.plotms(
        ms,
        xaxis="time",
        yaxis="elevation",
        coloraxis="field",
        antenna="ea01",
        plotfile=plotfile,
        overwrite=True,
        highres=True,
    )

    plotfile = os.path.join(path, "data_stream.png")
    print(f"\nplot antenna data stream: {plotfile}")
    casaplotms.plotms(
        ms,
        antenna="ea01",
        xaxis="time",
        yaxis="antenna2",
        plotrange=[-1, -1, 0, 26],
        coloraxis="field",
        highres=True,
        overwrite=True,
        plotfile=plotfile,
    )

    listfile = os.path.join(root, "output/listobs.txt")
    print(f"\nwriting listobs to file: {listfile}")
    casatasks.listobs(ms, listfile=listfile, overwrite=True)
    files.append(listfile)

    return files


@task(cache_key_fn=task_input_hash)
def setjy_model_amp_vs_uvdist(ms):
    """Plot amplitude vs uv-distance of the flux calibrator model.

    Parameters
    ----------
    ms : str
        path to measurement set
    field : str
        field name of primary calibrator

    Returns
    -------
    string
        file name of plot
    """

    root = os.path.dirname(ms)
    plotfile = os.path.join(root, "plots/calplots/setjy_model_amp_vs_uvdist.png")

    msmd = casatools.msmetadata()
    msmd.open(ms)
    field = msmd.fieldsforintent("CALIBRATE_FLUX#UNSPECIFIED")[0]

    print(f"\nplotting model amplitude vs. uv-distance: {plotfile}")

    casaplotms.plotms(
        ms,
        xaxis="uvdist",
        yaxis="amp",
        coloraxis="spw",
        field=str(field),
        ydatacolumn="model",
        plotfile=plotfile,
        overwrite=True,
        highres=True,
    )

    return plotfile


@task(cache_key_fn=task_input_hash)
def find_dead_ants_amp_vs_freq(ms, ant0=0, nants=27):
    """Plot ampltiude vs frequency for different baseline pairs
    formed by the same antenna. This will allow for the identification
    of antennas with low power (dead).

    Parameters
    ----------
    ms : str
        path to measurement set
    ant0 : int, optional
        index of antenna, by default 0
    nants : int, optional
        number of antennas, by default 27

    Returns
    -------
    list of str
        file names of plots

    """

    root = os.path.dirname(ms)
    plotfiles = []

    msmd = casatools.msmetadata()
    msmd.open(ms)
    fluxcal = msmd.fieldsforintent("CALIBRATE_FLUX#UNSPECIFIED")
    phasecal = msmd.fieldsforintent("CALIBRATE_PHASE#UNSPECIFIED")
    field_list = np.hstack([fluxcal, phasecal]).astype(str)
    fields = ""

    for field in field_list:
        fields += f"{field},"

    fields = fields[:-1]

    print(f"\nplotting data amplitude vs. frequency")

    for ant in range(nants):
        if ant != ant0:
            plotfile = os.path.join(
                root, f"plots/dataplots/find_dead_ants_amp_vs_freq_ant_{ant}.png"
            )
            casaplotms.plotms(
                ms,
                xaxis="freq",
                yaxis="amp",
                coloraxis="spw",
                antenna=f"{ant0},{ant}",
                field=fields,
                ydatacolumn="data",
                correlation="RR,LL",
                plotfile=plotfile,
                plotrange=[1, 2, 0, 1],
                avgtime="400",
                overwrite=True,
                highres=True,
            )
            plotfiles.append(plotfile)

    return plotfiles


def initcal(
    ms, field, nants, delay_init_table, delay_table, bandpass_init_table, bandpass_table
):
    """Plot gain tables and data after initial calibration.

    Parameters
    ----------
    ms : str
        measurement set
    field : str
        field
    nants : int
        number of antennas
    delay_init_table : str
        initial delay gain calibration table
    delay_table : str
        delay calibration table
    bandpass_init_table : str
        initial bandpass gain calibration table
    bandpass_table : str
        bandpass calibration table
    """

    print("\nplot calibration")

    root = os.path.dirname(ms)

    for ant in range(nants):
        ## BANDPASS ###
        # phase
        casaplotms.plotms(
            vis=bandpass_init_table,
            field=field,
            xaxis="time",
            yaxis="phase",
            coloraxis="spw",
            iteraxis="corr",
            gridcols=2,
            plotrange=[-1, -1, -180, 180],
            antenna=str(ant),
            highres=True,
            overwrite=True,
            plotfile=root
            + f"/plots/calplots/{bandpass_init_table}_phase_ant_{ant}.png",
        )
        # amplitude
        casaplotms.plotms(
            vis=bandpass_init_table,
            field=field,
            xaxis="time",
            yaxis="amp",
            coloraxis="spw",
            iteraxis="corr",
            gridcols=2,
            antenna=str(ant),
            highres=True,
            overwrite=True,
            plotfile=root + f"/plots/calplots/{bandpass_init_table}_amp_ant_{ant}.png",
        )
        # phase
        casaplotms.plotms(
            vis=bandpass_table,
            field=field,
            xaxis="freq",
            yaxis="phase",
            coloraxis="spw",
            iteraxis="corr",
            gridcols=2,
            antenna=str(ant),
            highres=True,
            overwrite=True,
            plotfile=root + f"/plots/calplots/{bandpass_table}_phase_ant_{ant}.png",
        )
        # amplitude
        casaplotms.plotms(
            vis=bandpass_table,
            field=field,
            xaxis="freq",
            yaxis="amp",
            coloraxis="spw",
            iteraxis="corr",
            gridcols=2,
            antenna=str(ant),
            highres=True,
            overwrite=True,
            plotfile=root + f"/plots/calplots/{bandpass_table}_amp_ant_{ant}.png",
        )
        ## DELAY ##
        casaplotms.plotms(
            delay_init_table,
            field=field,
            xaxis="time",
            yaxis="phase",
            coloraxis="spw",
            iteraxis="corr",
            gridcols=2,
            plotrange=[-1, -1, -180, 180],
            antenna=str(ant),
            highres=True,
            overwrite=True,
            plotfile=root + f"/plots/calplots/{delay_init_table}_ant_{ant}.png",
        )

    print("plot delay phase calibration")
    casaplotms.plotms(
        delay_table,
        field=field,
        xaxis="antenna1",
        yaxis="delay",
        coloraxis="baseline",
        iteraxis="corr",
        gridcols=2,
        highres=True,
        overwrite=True,
        plotfile=root + f"/plots/calplots/{delay_table}.png",
    )

    print("\nplot corrected data")
    casaplotms.plotms(
        vis=ms,
        field=field,
        xaxis="freq",
        yaxis="amp",
        coloraxis="antenna1",
        iteraxis="corr",
        ydatacolumn="corrected",
        gridcols=2,
        gridrows=2,
        highres=True,
        overwrite=True,
        plotfile=root + f"/plots/dataplots/fluxcal_init_cal_amp_vs_freq.png",
    )
    casaplotms.plotms(
        vis=ms,
        field=field,
        xaxis="freq",
        yaxis="phase",
        coloraxis="antenna1",
        iteraxis="corr",
        ydatacolumn="corrected",
        gridcols=2,
        gridrows=2,
        highres=True,
        overwrite=True,
        plotfile=root + f"/plots/dataplots/fluxcal_init_cal_phase_vs_freq.png",
    )
