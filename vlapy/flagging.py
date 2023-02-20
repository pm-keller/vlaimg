#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

flagging.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: VLA flagging routines.

"""

import os
import sys
import yaml
import casatasks
import casatools
import numpy as np

from prefect import task
from prefect.tasks import task_input_hash

sys.path.append(os.getcwd())
from vlapy import vladata

@task(cache_key_fn=task_input_hash)
def detflags(ms, quack, nchan, nspw, apriori):
    """Apply VLA deterministic flags. These include antennas not on source,
    shadowed antennas, scans with bad intents, autocorrelations, edge channels of spectral windows,
    edge channels of the baseband, clipping absolute zero values produced by the correlator and the
    first few integrations of a scan (quacking).

    Parameters
    ----------
    ms : str
        path to measurement set
    quack : int
        quack interval (s)
    nchan : int
        number of spw edge channels to flag
    nspw : int
        number of spectral windows
    apriori : dict
        apriori flags

    Returns
    -------
    list of str
        paths to summary files
    """

    print(f"\nundo previous flagging")
    versionnames = casatasks.flagmanager(ms, mode="list")

    if "before_detflags" in versionnames:
        casatasks.flagmanager(
            ms,
            mode="restore",
            versionname="before_detflags",
        )
    elif "flagdata_1" in versionnames:
        casatasks.flagmanager(
            ms,
            mode="restore",
            versionname="flagdata_1",
        )

    if len(versionnames) > 1:
        for i in versionnames:
            if i != "MS":
                casatasks.flagmanager(
                    ms,
                    mode="delete",
                    versionname=versionnames[i]["name"]
                )

    print("\nsave flag version: before_detflags")
    casatasks.flagmanager(ms, mode="save", versionname="before_detflags")

    root = os.path.dirname(ms)
    outpath = os.path.join(root, "output")

    summary_file_before = os.path.join(outpath, "detflag_summary_before.npy")
    print(f"\nwriting summary to: {summary_file_before}\n")
    summary_before = casatasks.flagdata(ms, mode="summary")
    np.save(summary_file_before, summary_before)

    plotfile = os.path.join(root, "plots/dataplots/flaggingreason_vs_time.png")

    print(f"\napplying online flags: {plotfile}")
    casatasks.flagcmd(
        ms,
        inpmode="table",
        reason="any",
        action="apply",
        useapplied=True,
        plotfile=plotfile,
        flagbackup=False,
    )

    print("\nclipping zeros")
    casatasks.flagdata(
        ms,
        mode="clip",
        clipzeros=True,
        reason="Zero Clipping",
        flagbackup=False,
    )

    print("\nflagging shadowed antennas")
    casatasks.flagdata(ms, mode="shadow", tolerance=0.0, flagbackup=False)

    print("\nflagging scan boundaries")
    casatasks.flagdata(
        ms,
        mode="quack",
        quackinterval=quack,
        quackmode="beg",
        reason="Quacking",
        flagbackup=False,
    )

    # Get time range of observation
    begin, end = vladata.get_obs_times(ms)

    print("\nflagging apriori bad antennas")
    for flag in apriori:
        timerange = apriori[flag]["time"].split("~")
        flag_begin, flag_end = vladata.casa_times_to_astropy(timerange)
        
        if ((flag_begin < end) & (flag_begin > begin)) | ((flag_end < end) & (flag_end > begin)) | ((flag_begin < begin) & (flag_end > end)):
            casatasks.flagdata(
                ms,
                mode="manual",
                antenna=apriori[flag]["antenna"],
                timerange=apriori[flag]["time"],
                reason=apriori[flag]["reason"],
                flagbackup=False,
            )

    print("\nflagging edges of spectral windows")
    for spw in range(nspw):
        casatasks.flagdata(
            ms,
            mode="manual",
            spw=f"{spw}:0~{nchan-1};{64-nchan}~63",
            reason="Flag SPW Edge Channels",
            flagbackup=False,
        )

    summary_file_after = os.path.join(outpath, "detflag_summary_after.npy")
    print(f"\nwriting summary to: {summary_file_after}\n")
    summary_after = casatasks.flagdata(ms, mode="summary")
    np.save(summary_file_after, summary_after)

    print("\nsave flag version: after_detflags")
    casatasks.flagmanager(ms, mode="save", versionname="after_detflags")

    return [summary_file_before, summary_file_after]
