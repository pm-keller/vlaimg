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
import h5py
import shutil
import casatasks
import numpy as np
from astropy.time import Time

from prefect import task
from prefect.tasks import task_input_hash

sys.path.append(os.getcwd())
from vlapy import vladata


def save(ms, name, when, field=""):
    """save flagversion and flagging summary

    Parameters
    ----------
    ms : str
        path to measurement set
    name : str
        name of processing step status
    when : str
        'before' or 'after' processing step
    field : str
        field to summarise
    """
    versionnames = vladata.get_versionnames(ms)
    root = os.path.dirname(ms)

    print(f"\nsave flag version: {when}_{name}_flags")
    if f"{when}_{name}_flags" in versionnames:
        casatasks.flagmanager(
            ms,
            mode="delete",
            versionname=f"{when}_{name}_flags",
        )
    casatasks.flagmanager(ms, mode="save", versionname=f"{when}_{name}_flags")

    print(f"\nwriting summary to: {name}_flags_summary_{when}.npy")
    summary_1 = casatasks.flagdata(
        ms,
        mode="summary",
        field=field,
        name=f"{when}_{name}_flagging",
    )
    np.save(root + f"/output/{name}_flags_summary_{when}.npy", summary_1)


# @task(cache_key_fn=task_input_hash)
def detflags(ms, quack, nchan, nspw, apriori, reapply=False):
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
    reapply : bool, optional
        if true, redo the flags
    """

    root = os.path.dirname(ms)

    summary_file_before = os.path.join(root, "output/detflag_summary_before.npy")
    summary_file_after = os.path.join(root, "output/detflag_summary_after.npy")

    # get flagging version names
    versionnames = vladata.get_versionnames(ms)

    if "after_detflags" in versionnames and not reapply:
        print("\nrestoring flag version: after_detflags")
        casatasks.flagmanager(
            ms,
            mode="restore",
            versionname=f"after_detflags",
        )
    else:
        print("\nsave flag version: before_detflags")
        if "before_detflags" in versionnames:
            casatasks.flagmanager(
                ms,
                mode="delete",
                versionname=f"before_detflags",
            )
        casatasks.flagmanager(ms, mode="save", versionname="before_detflags")

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

            if (
                ((flag_begin < end) & (flag_begin > begin))
                | ((flag_end < end) & (flag_end > begin))
                | ((flag_begin < begin) & (flag_end > end))
            ):
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

        print(f"\nwriting summary to: {summary_file_after}\n")
        summary_after = casatasks.flagdata(ms, mode="summary")
        np.save(summary_file_after, summary_after)

        print("\nsave flag version: after_detflags")
        if "after_detflags" in versionnames:
            casatasks.flagmanager(
                ms,
                mode="delete",
                versionname=f"after_detflags",
            )
        casatasks.flagmanager(ms, mode="save", versionname="after_detflags")


# @task(cache_key_fn=task_input_hash)
def autoroutine(
    ms,
    field,
    target="",
    rnd=0,
    devscale=5,
    argdevscale=1e6,
    cutoff=4,
    grow=75,
    datacolumn="corrected",
    overwrite=False,
):
    """Flagging routine using CASA's RFlag and TFCrop.
    Measurement set must be calibrated, i.e. have a corrected data column.

    Parameters
    ----------
    ms : str
        path to measurement set
    field : str
        field to run flagging routine on
    target : str, optional
        name of flagging targets (e.g. 'calibrators'), by default ""
    rnd : int, optional
        flagging round, by default 0
    devscale : int, optional
        devscale for RFlag, by default 5
    argdevscale : int, optional
        devscale for RFlag on phases, by default 1e6
    cutoff : int, optional
        cutoff for TFCrop, by default 4
    grow : float, optional
        grow flags if occupancy exceeds this threshold in percent, by degault 75
    datacolumn : str, optional
        if model is available, set this to 'residual', by default "residual"
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    versionnames = vladata.get_versionnames(ms)

    if f"after_{target}_round_{rnd}_flags" in versionnames and not overwrite:
        print(f"\nrestoring flag version: after_{target}_round_{rnd}_flags")
        casatasks.flagmanager(
            ms, mode="restore", versionname=f"after_{target}_round_{rnd}_flags"
        )
    else:
        save(ms, f"{target}_round_{rnd}", "before", field)

        print(
            f"\nsplit time-averaged data: {ms+f'.before_flagging.{target}_round_{rnd}.averaged'}"
        )

        if os.path.exists(ms + f".before_flagging.{target}_round_{rnd}.averaged"):
            shutil.rmtree(ms + f".before_flagging.{target}_round_{rnd}.averaged")

        casatasks.mstransform(
            ms,
            outputvis=ms + f".before_flagging.{target}_round_{rnd}.averaged",
            field=field,
            correlation="RR,LL",
            datacolumn="corrected",
            keepflags=False,
            timeaverage=True,
            timebin="1e8",
            timespan="scan",
            reindex=False,
        )

        print(f"\ncompute noise thresholds for ABS_RL correlation")
        thresholds = casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="ABS_RL",
            datacolumn="corrected",
            timedevscale=devscale,
            freqdevscale=devscale,
            action="calculate",
            extendflags=False,
            flagbackup=False,
        )

        print(
            f"\nRFlag on ABS_RL with timedevscale={devscale} and freqdevscale={devscale}"
        )
        casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="ABS_RL",
            datacolumn="corrected",
            timedev=thresholds["report0"]["timedev"],
            freqdev=thresholds["report0"]["freqdev"],
            timedevscale=devscale,
            freqdevscale=devscale,
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growtime=100.0,
            growfreq=100.0,
            action="apply",
            extendpols=True,
            extendflags=False,
            flagbackup=False,
        )

        print(f"\ncompute noise thresholds for ABS_LR correlation")
        thresholds = casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="ABS_LR",
            datacolumn="corrected",
            timedevscale=devscale,
            freqdevscale=devscale,
            action="calculate",
            extendflags=False,
            flagbackup=False,
        )

        print(
            f"\nRFlag on ABS_LR with timedevscale={devscale} and freqdevscale={devscale}"
        )
        casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="ABS_LR",
            datacolumn="corrected",
            timedev=thresholds["report0"]["timedev"],
            freqdev=thresholds["report0"]["freqdev"],
            timedevscale=devscale,
            freqdevscale=devscale,
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growtime=100.0,
            growfreq=100.0,
            action="apply",
            extendpols=True,
            extendflags=False,
            flagbackup=False,
        )

        print(f"\ncompute noise thresholds for REAL_RR correlation")
        thresholds = casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="REAL_RR",
            datacolumn=datacolumn,
            timedevscale=devscale,
            freqdevscale=devscale,
            action="calculate",
            extendflags=False,
            flagbackup=False,
        )

        print(
            f"\nRFlag on REAL_RR with timedevscale={devscale} and freqdevscale={devscale}"
        )
        casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="REAL_RR",
            datacolumn=datacolumn,
            timedev=thresholds["report0"]["timedev"],
            freqdev=thresholds["report0"]["freqdev"],
            timedevscale=devscale,
            freqdevscale=devscale,
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growtime=100.0,
            growfreq=100.0,
            action="apply",
            extendpols=True,
            extendflags=False,
            flagbackup=False,
        )

        print(f"\ncompute noise thresholds for REAL_LL correlation")
        thresholds = casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="REAL_LL",
            datacolumn=datacolumn,
            timedevscale=devscale,
            freqdevscale=devscale,
            action="calculate",
            extendflags=False,
            flagbackup=False,
        )

        print(
            f"\nRFlag on REAL_LL with timedevscale={devscale} and freqdevscale={devscale}"
        )
        casatasks.flagdata(
            ms,
            mode="rflag",
            field=field,
            correlation="REAL_LL",
            datacolumn=datacolumn,
            timedev=thresholds["report0"]["timedev"],
            freqdev=thresholds["report0"]["freqdev"],
            timedevscale=devscale,
            freqdevscale=devscale,
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growtime=100.0,
            growfreq=100.0,
            action="apply",
            extendpols=True,
            extendflags=False,
            flagbackup=False,
        )

        if argdevscale < 1e6:
            print(f"\ncompute noise thresholds for phase flagging on RR correlation")
            thresholds = casatasks.flagdata(
                ms,
                mode="rflag",
                field=field,
                correlation="RR_ARG",
                datacolumn=datacolumn,
                timedevscale=argdevscale,
                freqdevscale=argdevscale,
                action="calculate",
                extendflags=False,
                flagbackup=False,
            )

            print(
                f"\nRFlag on phase of RR correlation with timedevscale={devscale} and freqdevscale={devscale}"
            )
            casatasks.flagdata(
                ms,
                mode="rflag",
                field=field,
                correlation="RR_ARG",
                datacolumn=datacolumn,
                timedev=thresholds["report0"]["timedev"],
                freqdev=thresholds["report0"]["freqdev"],
                timedevscale=argdevscale,
                freqdevscale=argdevscale,
                action="apply",
                extendflags=False,
                flagbackup=False,
            )

            print(f"\nextend flags across polarisations")
            casatasks.flagdata(
                ms,
                mode="extend",
                field=field,
                growtime=100.0,
                growfreq=100.0,
                action="apply",
                extendpols=True,
                extendflags=False,
                flagbackup=False,
            )

            print(f"\ncompute noise thresholds for phase flagging on LL correlation")
            thresholds = casatasks.flagdata(
                ms,
                mode="rflag",
                field=field,
                correlation="LL_ARG",
                datacolumn=datacolumn,
                timedevscale=argdevscale,
                freqdevscale=argdevscale,
                action="calculate",
                extendflags=False,
                flagbackup=False,
            )

            print(
                f"\nRFlag on phase of LL correlation with timedevscale={devscale} and freqdevscale={devscale}"
            )
            casatasks.flagdata(
                ms,
                mode="rflag",
                field=field,
                correlation="LL_ARG",
                datacolumn=datacolumn,
                timedev=thresholds["report0"]["timedev"],
                freqdev=thresholds["report0"]["freqdev"],
                timedevscale=argdevscale,
                freqdevscale=argdevscale,
                action="apply",
                extendflags=False,
                flagbackup=False,
            )

            print(f"\nextend flags across polarisations")
            casatasks.flagdata(
                ms,
                mode="extend",
                field=field,
                growtime=100.0,
                growfreq=100.0,
                action="apply",
                extendpols=True,
                extendflags=False,
                flagbackup=False,
            )

        print(f"\nTFCrop on ABS_LR with timecutoff=4, freqcutoff=4, ntime=5")
        casatasks.flagdata(
            ms,
            mode="tfcrop",
            field=field,
            correlation="ABS_LR",
            datacolumn="corrected",
            ntime=5.0,
            timecutoff=cutoff,
            freqcutoff=cutoff,
            freqfit="line",
            flagdimension="freq",
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growtime=100.0,
            growfreq=100.0,
            action="apply",
            extendpols=True,
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nTFCrop on ABS_RL with timecutoff=4, freqcutoff=4, ntime=5")
        casatasks.flagdata(
            ms,
            mode="tfcrop",
            field=field,
            correlation="ABS_RL",
            datacolumn="corrected",
            ntime=5.0,
            timecutoff=cutoff,
            freqcutoff=cutoff,
            freqfit="line",
            flagdimension="freq",
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growtime=100.0,
            growfreq=100.0,
            action="apply",
            extendpols=True,
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nTFCrop on ABS_LL with timecutoff=4, freqcutoff=4, ntime=5")
        casatasks.flagdata(
            ms,
            mode="tfcrop",
            field=field,
            correlation="ABS_LL",
            datacolumn="corrected",
            ntime=5.0,
            timecutoff=cutoff,
            freqcutoff=cutoff,
            freqfit="line",
            flagdimension="freq",
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growtime=100.0,
            growfreq=100.0,
            action="apply",
            extendpols=True,
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nTFCrop on ABS_RR with timecutoff=4, freqcutoff=4, ntime=5")
        casatasks.flagdata(
            ms,
            mode="tfcrop",
            field=field,
            correlation="ABS_RR",
            datacolumn="corrected",
            ntime=5.0,
            timecutoff=cutoff,
            freqcutoff=cutoff,
            freqfit="line",
            flagdimension="freq",
            action="apply",
            extendflags=False,
            flagbackup=False,
        )

        print(f"\nextend flags across polarisations")
        casatasks.flagdata(
            ms,
            mode="extend",
            field=field,
            growfreq=grow,
            growtime=grow,
            action="apply",
            extendpols=True,
            extendflags=True,
            combinescans=False,
            flagbackup=False,
        )

        save(ms, f"{target}_round_{rnd}", "after", field)

        print(
            f"\nsplit time-averaged {target} fields to: {ms+f'.after_flagging.{target}_round_{rnd}.averaged'}"
        )

        if os.path.exists(ms + f".after_flagging.{target}_round_{rnd}.averaged"):
            shutil.rmtree(ms + f".after_flagging.{target}_round_{rnd}.averaged")

        casatasks.mstransform(
            ms,
            outputvis=ms + f".after_flagging.{target}_round_{rnd}.averaged",
            field=field,
            correlation="RR,LL",
            datacolumn="corrected",
            keepflags=False,
            timeaverage=True,
            timebin="1e8",
            timespan="scan",
            reindex=False,
        )


# @task(cache_key_fn=task_input_hash)
def madclip(ms, fields, target, spws, nsig=4, tavg=False, overwrite=False):
    """Clip using median absolute deviation

    Parameters
    ----------
    ms : str
        path to measurement set
    fields : str
        fields to clip on
    spws : list of str
        spectral windows to clip on
    nsig : int, optional
        number of MADs outside of which to clip, by default 4
    tavg : bool, optional
        average in time, by default False
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    versionnames = vladata.get_versionnames(ms)
    if f"after_{target}_MAD_clipping_flags" in versionnames and not overwrite:
        print(f"\nrestoring flag version: after_{target}_MAD_clipping_flags")
        casatasks.flagmanager(
            ms, mode="restore", versionname=f"after_{target}_MAD_clipping_flags"
        )
    else:
        save(ms, f"{target}_MAD_clipping", "before", fields)

        print(f"\nclipping {nsig}*MAD outliers on {target}")

        for field in fields.split(","):
            for spw in spws:
                for corr in ["RR", "LL"]:
                    # get visibility statistics
                    stat = casatasks.visstat(
                        ms,
                        timeaverage=tavg,
                        spw=spw,
                        field=field,
                        correlation=corr,
                        axis="amp",
                        datacolumn="corrected",
                    )

                    id = [key for key in stat.keys()][0]
                    stat = stat[id]

                    print(
                        field,
                        spw,
                        corr,
                        stat["median"],
                        stat["medabsdevmed"],
                        "ABS_" + corr,
                    )

                    # clip outside nsig median absolute deviations
                    casatasks.flagdata(
                        ms,
                        mode="clip",
                        action="apply",
                        datacolumn="corrected",
                        spw=spw,
                        field=field,
                        clipminmax=[
                            stat["median"] - nsig * stat["medabsdevmed"],
                            stat["median"] + nsig * stat["medabsdevmed"],
                        ],
                        correlation="ABS_" + corr,
                        clipoutside=True,
                        flagbackup=False,
                    )

        save(ms, f"{target}_MAD_clipping", "after", fields)


# @task(cache_key_fn=task_input_hash)
def manual(ms, flags, overwrite=False):
    """Apply manual flags

    Parameters
    ----------
    ms : str
        path to measurement set
    flags : dict
        manual flags. Each entry should contain either a "time" or a "spw" and a "reason".
    overwrite : bool, optional
        if true, overwrite existing plots, by default False
    """

    versionnames = vladata.get_versionnames(ms)
    if f"after_manual_flags" in versionnames and not overwrite:
        print(f"restoring flag version: after_manual_flags")
        casatasks.flagmanager(ms, mode="restore", versionname=f"after_manual_flags")
    else:
        save(ms, "manual", "before")

        print("\napplying manual flags")
        for flag in flags:
            print(flags[flag]["reason"])

            if "ant" in flags[flag]:
                ant = flags[flag]["ant"]
            else:
                ant = "*"

            if "time" in flags[flag]:
                casatasks.flagdata(
                    ms,
                    antenna=ant,
                    timerange=flags[flag]["time"],
                    reason=flags[flag]["reason"],
                    flagbackup=False,
                )
            elif "spw" in flags[flag]:
                casatasks.flagdata(
                    ms,
                    antenna=ant,
                    spw=flags[flag]["spw"],
                    reason=flags[flag]["reason"],
                    flagbackup=False,
                )

        save(ms, "manual", "after")


def zclip(ms, nsig, overwrite=False):
    """Clip based on modified Z-score

    Parameters
    ----------
    ms : str
        path to measurement set
    nsig : int
        number of sigmas above which to clip
    overwrite : bool, optional
        if true, overwrite existing flags, by default False
    """

    save(ms, "zscore", "before")

    versionnames = vladata.get_versionnames(ms)
    if f"after_zscore_flags" in versionnames and not overwrite:
        print(f"restoring flag version: after_zscore_flags")
        casatasks.flagmanager(ms, mode="restore", versionname=f"after_zscore_flags")
    else:
        print("\nmodified Z-score flagging")

        root = os.path.dirname(ms)
        z_score_path = os.path.join(root, "output", "z_score_corrected.h5")

        with h5py.File(z_score_path, "r") as f:
            z_score = f["z-score avg"][()]
            flags = f["flags avg"][()]
            time_array = f["time array"][()]

        # time array
        tisot = np.unique(Time(time_array, format="jd", scale="utc").isot)

        # apply masks
        z_score = np.ma.masked_array(z_score, mask=flags)

        # flag
        idx = np.where(np.abs(z_score) > nsig)

        for cnt, (i, j) in enumerate(zip(idx[0], idx[1])):
            spw = str(j // 64) + ":" + str(j % 64)
            N = len(tisot)

            time1 = tisot[max(i - 1, 0)][:-4].replace("T", "/").replace("-", "/")
            time2 = tisot[min(i + 1, N-1)][:-4].replace("T", "/").replace("-", "/")
            timerange = time1 + "~" + time2

            print(spw, timerange)

            casatasks.flagdata(
                ms,
                mode="manual",
                reason="Z-score",
                spw=spw,
                timerange=timerange,
                flagbackup=False,
            )

        save(ms, "zscore", "after")
