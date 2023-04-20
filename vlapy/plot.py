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
import sys
import glob
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

sys.path.append(os.getcwd())
from vlapy import vladata


# @task(cache_key_fn=task_input_hash)
def plotobs(ms, overwrite=False):
    """Make some plots that describe the observation:

    - Elevation vs. Time
    - Array Layout
    - Antenna Data Stream

    Save CASA's listobs output to a text file.

    Parameters
    ----------
    ms : str
        path to measurement set
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    root = os.path.dirname(ms)

    plotfile = os.path.join(root, "plots/obsplots/antlayout.png")

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
        print(f"\nplot antenna layout: {plotfile}")
        casatasks.plotants(ms, figfile=plotfile)

    plotfile = os.path.join(root, "plots/obsplots/elevation_vs_time.png")

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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

    plotfile = os.path.join(root, "plots/obsplots/data_stream.png")

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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

    if not os.path.exists(listfile) or overwrite:
        print(f"\nwriting listobs to file: {listfile}")
        casatasks.listobs(ms, listfile=listfile, overwrite=True)


# @task(cache_key_fn=task_input_hash)
def setjy_model_amp_vs_uvdist(ms, overwrite=False):
    """Plot amplitude vs uv-distance of the flux calibrator model.

    Parameters
    ----------
    ms : str
        path to measurement set
    overwrite : bool, optional
        if true, overwrite existing plot, by default False
    """

    root = os.path.dirname(ms)
    plotfile = os.path.join(root, "plots/calplots/setjy_model_amp_vs_uvdist.png")

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
        field = vladata.get_field_names(ms)["fluxcal"]

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


# @task(cache_key_fn=task_input_hash)
def find_dead_ants_amp_vs_freq(ms, ant0=0, nants=27, overwrite=False):
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
    overwrite : bool, optional
        if true, overwrite existing plot, by default False
    """

    root = os.path.dirname(ms)

    field_dict = vladata.get_field_names(ms)
    fields = field_dict["calibrators"]

    print(f"\nplotting data amplitude vs. frequency")

    for ant in range(nants):
        if ant != ant0:
            plotfile = os.path.join(
                root, f"plots/dataplots/find_dead_ants_amp_vs_freq_ant_{ant}.png"
            )

            if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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


# @task(cache_key_fn=task_input_hash)
def single_chans_amp_vs_time(ms, chans, overwrite=False):
    """Plot calibration channels amplitude vs. time.

    Parameters
    ----------
    ms : str
        path to measurement set
    chans : str
        spectral window string
    overwrite : bool, optional
        if true, overwrite existing plot, by default False
    """

    root = os.path.dirname(ms)
    plotfile = root + f"/plots/dataplots/single_chans_amp_vs_time.png"

    fields = vladata.get_field_names(ms)["calibrators"]

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
        print(f"\nplotting the calibration channels: {plotfile}")
        casaplotms.plotms(
            ms,
            xaxis="time",
            yaxis="amp",
            correlation="RR, LL",
            coloraxis="spw",
            iteraxis="antenna",
            spw=chans,
            field=fields,
            avgbaseline=True,
            scalar=True,
            highres=True,
            plotfile=plotfile,
            overwrite=True,
        )


# @task(cache_key_fn=task_input_hash)
def initcal(
    ms,
    nants,
    nspw,
    delay_init_table,
    delay_table,
    bandpass_init_table,
    bandpass_table,
    rnd=0,
    overwrite=False,
):
    """Plot gain tables and data after initial calibration.

    Parameters
    ----------
    ms : str
        path to measurement set
    nants : int
        number of antennas
    nspw : int
        number of spectral windows
    delay_init_table : str
        initial delay gain calibration table
    delay_table : str
        delay calibration table
    bandpass_init_table : str
        initial bandpass gain calibration table
    bandpass_table : str
        bandpass calibration table
    overwrite : bool, optional
        if true, overwrite existing plot, by default False
    """

    print("\nplot calibration")

    root = os.path.dirname(ms)

    # get field names
    field_dict = vladata.get_field_names(ms)
    field = field_dict["fluxcal"]

    for ant in range(nants):
        ## BANDPASS ###
        # phase
        plotfile = os.path.join(
            root,
            f"plots/calplots/{bandpass_init_table.split('/')[-1]}_phase_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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
                plotfile=plotfile,
            )
        # amplitude
        plotfile = os.path.join(
            root,
            f"plots/calplots/{bandpass_init_table.split('/')[-1]}_amp_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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
                plotfile=plotfile,
            )
        # phase
        plotfile = os.path.join(
            root, f"plots/calplots/{bandpass_table.split('/')[-1]}_phase_ant_{ant}.png"
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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
                plotfile=plotfile,
            )
        # amplitude
        plotfile = os.path.join(
            root, f"plots/calplots/{bandpass_table.split('/')[-1]}_amp_ant_{ant}.png"
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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
                plotfile=plotfile,
            )
        ## DELAY ##
        plotfile = os.path.join(
            root, f"plots/calplots/{delay_init_table.split('/')[-1]}_ant_{ant}.png"
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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
                plotfile=plotfile,
            )

    for spw in range(nspw):
        plotfile = os.path.join(
            root, f"plots/calplots/{bandpass_table.split('/')[-1]}_phase_spw_{spw}.png"
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=bandpass_table,
                field=field,
                xaxis="channel",
                yaxis="phase",
                coloraxis="antenna1",
                iteraxis="corr",
                spw=str(spw),
                gridcols=2,
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )
        plotfile = os.path.join(
            root, f"plots/calplots/{bandpass_table.split('/')[-1]}_amp_spw_{spw}.png"
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=bandpass_table,
                field=field,
                xaxis="channel",
                yaxis="amp",
                coloraxis="antenna1",
                iteraxis="corr",
                spw=str(spw),
                gridcols=2,
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )
        plotfile = os.path.join(
            root,
            f"plots/dataplots/fluxcal_initcal_round_{rnd}_amp_vs_freq_avg_spw_{spw}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=ms,
                field=field,
                xaxis="channel",
                yaxis="amp",
                coloraxis="antenna1",
                spw=str(spw),
                iteraxis="corr",
                ydatacolumn="corrected",
                avgtime="720",
                avgbaseline=True,
                scalar=True,
                gridcols=2,
                gridrows=2,
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )

    print("\nplot delay calibration")
    plotfile = os.path.join(root, f"plots/calplots/{delay_table.split('/')[-1]}.png")
    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
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
            plotfile=plotfile,
        )

    print("\nplot corrected data")
    plotfile = os.path.join(
        root, f"plots/dataplots/fluxcal_initcal_round_{rnd}_amp_vs_freq.png"
    )
    if (len(glob.glob(plotfile[:-4] + "_Corr*")) == 0) or overwrite:
        casaplotms.plotms(
            vis=ms,
            field=field,
            xaxis="freq",
            yaxis="amp",
            coloraxis="antenna1",
            iteraxis="corr",
            ydatacolumn="corrected",
            avgtime="500",
            scalar=True,
            gridcols=2,
            gridrows=2,
            highres=True,
            overwrite=True,
            plotfile=plotfile,
        )
    plotfile = os.path.join(
        root, f"plots/dataplots/fluxcal_initcal_round_{rnd}_phase_vs_freq.png"
    )
    if (len(glob.glob(plotfile[:-4] + "_Corr*")) == 0) or overwrite:
        casaplotms.plotms(
            vis=ms,
            field=field,
            xaxis="freq",
            yaxis="phase",
            coloraxis="antenna1",
            iteraxis="corr",
            ydatacolumn="corrected",
            avgtime="500",
            scalar=True,
            gridcols=2,
            gridrows=2,
            highres=True,
            overwrite=True,
            plotfile=plotfile,
        )


# @task(cache_key_fn=task_input_hash)
def flagging_before_after(ms, field, target="fluxcal", overwrite=False):
    """Plot data amplitude vs frequency before and after flagging.

    Parameters
    ----------
    ms : str
        path to measurement set
    field : str
        field name
    target : str, optional
        name of flagging targets (e.g. calibrators), by default "fluxcal"
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    root = os.path.dirname(ms)

    print(f"\nplot data before and after flagging on {target}")

    plotfile = root + f"/plots/dataplots/before_{target}_flagging_amp_vs_freq.png"

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
        print(plotfile)
        casaplotms.plotms(
            ms + f".before_flagging.{target}.averaged",
            field=field,
            xaxis="freq",
            yaxis="amp",
            coloraxis="antenna1",
            iteraxis="corr",
            ydatacolumn="corrected",
            gridcols=2,
            highres=True,
            overwrite=True,
            plotfile=plotfile,
        )

    plotfile = root + f"/plots/dataplots/after_{target}_flagging_amp_vs_freq.png"

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
        print(plotfile)
        casaplotms.plotms(
            ms + f".after_flagging.{target}.averaged",
            field=field,
            xaxis="freq",
            yaxis="amp",
            coloraxis="antenna1",
            iteraxis="corr",
            ydatacolumn="corrected",
            gridcols=2,
            highres=True,
            overwrite=True,
            plotfile=plotfile,
        )

    plotfile = root + f"/plots/dataplots/before_{target}_flagging_phase_vs_freq.png"

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
        print(plotfile)
        casaplotms.plotms(
            ms + f".before_flagging.{target}.averaged",
            field=field,
            xaxis="freq",
            yaxis="phase",
            coloraxis="antenna1",
            iteraxis="corr",
            ydatacolumn="corrected",
            gridcols=2,
            highres=True,
            overwrite=True,
            plotfile=plotfile,
        )

    plotfile = root + f"/plots/dataplots/after_{target}_flagging_phase_vs_freq.png"

    if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
        print(plotfile)
        casaplotms.plotms(
            ms + f".after_flagging.{target}.averaged",
            field=field,
            xaxis="freq",
            yaxis="phase",
            coloraxis="antenna1",
            iteraxis="corr",
            ydatacolumn="corrected",
            gridcols=2,
            highres=True,
            overwrite=True,
            plotfile=plotfile,
        )


# @task(cache_key_fn=task_input_hash)
def calibrator_models(ms, overwrite=False):
    """Plot model amplitude vs frequency of calibrators

    Parameters
    ----------
    ms : str
        path to measurement set
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    root = os.path.dirname(ms)

    # get field names
    field_dict = vladata.get_field_names(ms)
    field = field_dict["phasecal"]

    plotfile_amp = root + f"/plots/dataplots/model_amp_vs_freq.png"

    if (not os.path.exists(plotfile_amp)) or overwrite:
        print(f"\nplot calibrator models amplitude vs frequency: {plotfile_amp}")
        casaplotms.plotms(
            ms,
            xaxis="freq",
            yaxis="amp",
            coloraxis="field",
            correlation="LL,RR",
            field=field,
            ydatacolumn="model",
            avgtime="500",
            scalar=True,
            highres=True,
            overwrite=True,
            plotfile=plotfile_amp,
        )

    plotfile_phase = root + f"/plots/dataplots/model_amp_vs_freq_residual.png"

    if (not os.path.exists(plotfile_phase)) or overwrite:
        print(f"\nplot calibrator models phase vs frequency: {plotfile_phase}")
        casaplotms.plotms(
            ms,
            xaxis="freq",
            yaxis="amp",
            coloraxis="field",
            correlation="LL,RR",
            field=field,
            ydatacolumn="residual",
            avgtime="500",
            avgchannel="64",
            scalar=True,
            highres=True,
            overwrite=True,
            plotfile=plotfile_phase,
        )


# @task(cache_key_fn=task_input_hash)
def fluxboot_gains(
    ms, nants, short_gain_table, long_gain_table, flux_gain_table, overwrite=False
):
    """Plot the calibration tables used for the flux bootstrapping

    Parameters
    ----------
    ms : str
        path to measurement set
    nants : int
        number of antennas
    short_gain_table : str
        gain table derived on short time intervals
    long_gain_table : str
        gain table derived on long time intervals with solnorm=True
    flux_gain_table : str
        final phase and amplitude gain table before spectral index fitting
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    print("\nplot fluxboot gain calibration")

    root = os.path.dirname(ms)

    # get field names
    field_dict = vladata.get_field_names(ms)
    field = field_dict["phasecal"]

    for ant in range(nants):
        # short interval gains
        # phase
        plotfile = os.path.join(
            root,
            f"plots/calplots/{short_gain_table.split('/')[-1]}_phase_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=short_gain_table,
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
                plotfile=plotfile,
            )
        # long interval gains
        plotfile = os.path.join(
            root,
            f"plots/calplots/{long_gain_table.split('/')[-1]}_phase_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=long_gain_table,
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
                plotfile=plotfile,
            )
        # amplitude
        plotfile = os.path.join(
            root,
            f"plots/calplots/{long_gain_table.split('/')[-1]}_amp_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=long_gain_table,
                field=field,
                xaxis="time",
                yaxis="amp",
                coloraxis="spw",
                iteraxis="corr",
                gridcols=2,
                antenna=str(ant),
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )
        # flux gains
        # phase
        plotfile = os.path.join(
            root,
            f"plots/calplots/{flux_gain_table.split('/')[-1]}_phase_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=flux_gain_table,
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
                plotfile=plotfile,
            )
        # amplitude
        plotfile = os.path.join(
            root,
            f"plots/calplots/{flux_gain_table.split('/')[-1]}_amp_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=flux_gain_table,
                field=field,
                xaxis="time",
                yaxis="amp",
                coloraxis="spw",
                iteraxis="corr",
                gridcols=2,
                antenna=str(ant),
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )


# @task(cache_key_fn=task_input_hash)
def finalcal(
    ms,
    nants,
    flux_phase_table,
    short_gain_table,
    amp_gain_table,
    phase_gain_table,
    overwrite=False,
):
    """Plot the final amplitude and phase calibration solutions

    Parameters
    ----------
    ms : str
        path to measurement set
    nants : int
        number of antennas
    flux_phase_table : str
        flux calibrator phase gain table
    short_gain_table : str
        gain table derived on short time intervals
    amp_gain_table : str
        amplitude gain table
    phase_gain_table : str
        phase gain table
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    print("\nplot final calibration")

    root = os.path.dirname(ms)

    # get field names
    field_dict = vladata.get_field_names(ms)
    field = field_dict["phasecal"]

    for ant in range(nants):
        # flux calibrator gain phases
        plotfile = os.path.join(
            root,
            f"plots/calplots/{flux_phase_table.split('/')[-1]}_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=flux_phase_table,
                field=field_dict["fluxcal"],
                xaxis="frequency",
                yaxis="phase",
                coloraxis="spw",
                iteraxis="corr",
                gridcols=2,
                plotrange=[-1, -1, -180, 180],
                antenna=str(ant),
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )
        # short interval gains
        # phase
        plotfile = os.path.join(
            root,
            f"plots/calplots/{short_gain_table.split('/')[-1]}_phase_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=short_gain_table,
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
                plotfile=plotfile,
            )
        # long interval gains
        plotfile = os.path.join(
            root,
            f"plots/calplots/{amp_gain_table.split('/')[-1]}_phase_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=amp_gain_table,
                field=field,
                xaxis="time",
                yaxis="phase",
                coloraxis="spw",
                iteraxis="corr",
                gridcols=2,
                antenna=str(ant),
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )
        # amplitude
        plotfile = os.path.join(
            root,
            f"plots/calplots/{amp_gain_table.split('/')[-1]}_amp_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=amp_gain_table,
                field=field,
                xaxis="time",
                yaxis="amp",
                coloraxis="spw",
                iteraxis="corr",
                gridcols=2,
                antenna=str(ant),
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )
        # final phase
        plotfile = os.path.join(
            root,
            f"plots/calplots/{phase_gain_table.split('/')[-1]}_phase_ant_{ant}.png",
        )
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                vis=phase_gain_table,
                field=field,
                xaxis="time",
                yaxis="phase",
                coloraxis="spw",
                iteraxis="corr",
                gridcols=2,
                antenna=str(ant),
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )


# @task(cache_key_fn=task_input_hash)
def summary(ms, overwrite=False):
    """Make summary plots:
    amplitude/phase vs frequency for all calibrators and targets.

    Parameters
    ----------
    ms : str
        path to measurement set
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    print("\nsummary plots")

    root = os.path.dirname(ms)

    msmd = casatools.msmetadata()
    msmd.open(ms)

    fields = msmd.fieldnames()

    for field in fields:
        print(field)

        plotfile = root + f"/plots/dataplots/{field}_amp_vs_uvdist_corrected.png"
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                ms,
                xaxis="uvwave",
                yaxis="amp",
                ydatacolumn="corrected",
                field=field,
                correlation="LL,RR",
                coloraxis="spw",
                avgtime="500",
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )

        plotfile = root + f"/plots/dataplots/{field}_amp_vs_freq_corrected.png"
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                ms,
                xaxis="freq",
                yaxis="amp",
                ydatacolumn="corrected",
                field=field,
                correlation="LL,RR",
                coloraxis="antenna1",
                avgtime="500",
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )

        plotfile = root + f"/plots/dataplots/{field}_phase_vs_freq_corrected.png"
        if (len(glob.glob(plotfile[:-4] + "*")) == 0) or overwrite:
            casaplotms.plotms(
                ms,
                xaxis="freq",
                yaxis="phase",
                ydatacolumn="corrected",
                field=field,
                correlation="LL,RR",
                coloraxis="antenna1",
                avgtime="500",
                highres=True,
                overwrite=True,
                plotfile=plotfile,
            )
