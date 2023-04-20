#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

inspect.py

Created on: 2023/01/18
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Tools for inspecting VLA L-Band data for RFI and malfunctioning antennas.

"""

import os
import h5py
import numpy as np
import matplotlib.pyplot as plt

import sys

sys.path.append("/DATA/CARINA_3/kel334/19A-056/vlaimg")

from vlapy import vladata
from vlapy.rfi_freq import (
    protected_freq,
    vla_internal,
)


def get_mod_z_score_data(
    ms, masked=True, data_column="DATA", ntimes="*", overwrite=False
):
    """
    Parameters
    ----------
    ms : str
        path to measurement set
    masked : bool, optional
        if true, apply flags, by default True
    data_column : str, optional
        data column to use for the z-score computation, by default "DATA"
    ntimes : list, optional
        number of time integrations per scan, by default "*"
    overwrite : bool, optional
        if true, overwrite existing data, by default False
    """

    root = os.path.dirname(ms)
    path = path = os.path.join(root, f"output/z_score_{data_column.lower()}.h5")

    # save some metadata to file
    ntimes = vladata.get_ntimes(ms)

    if not os.path.exists(path) or overwrite:
        # get data array from data file
        print(f"\nloading measurement set with pyuvdata: {ms}")
        uvd = vladata.get_uvdata(ms, polarizations=["RR", "LL"])
        antpairs = uvd.get_antpairs()

        """
        # compute average across polarisation and antenna pairs
        print("compute data average")
        for i, antpair in enumerate(antpairs):
            data_antpair = np.abs(uvd.get_data(*antpair))
            flags_antpair = uvd.get_flags(*antpair)
            data_antpair = np.ma.masked_array(data_antpair, maks=flags_antpair)
            
            if i == 0:
                data_array_avg = np.ma.mean(data_antpair, axis=-1)
                flag_array_sum = np.ma.sum(flags_antpair, axis=-1)
            else:
                data_array_avg += np.ma.mean(data_antpair, axis=-1)
                flag_array_sum += np.ma.sum(flags_antpair, axis=-1)

        data_array_avg /= flag_array_sum
        """
        data_array = np.abs(vladata.get_data_array(uvd, data_column))

        print("apply mask")
        if masked:
            flags = vladata.get_flag_array(uvd, data_column)
            data_array = np.ma.masked_array(data_array, mask=flags)

        # average amplitudes across polarisations and baselines
        print("compute average")
        data_array_avg = np.ma.mean(data_array, axis=(0, 1))
        
        # get metadata from file
        freq_array = uvd.freq_array[0] * 1e-6
        dt = uvd.integration_time[0]
        time_array = uvd.time_array
        ant_pairs = uvd.get_antpairs()

        data_array = np.moveaxis(data_array, 2, 0)

        if ntimes == "*":
            ntimes = [
                data_array_avg.shape[0],
            ]

        print("\ncomputing modified Z-score")
        for i in range(len(ntimes)):
            # scan boundary indices
            idx1 = int(np.sum(ntimes[:i]))
            idx2 = int(np.sum(ntimes[: i + 1]))

            # subtract median from data
            data_array[idx1:idx2] -= np.ma.median(data_array[idx1:idx2], axis=0)
            data_array_avg[idx1:idx2] -= np.ma.median(data_array_avg[idx1:idx2], axis=0)

            # divide by median absolute deviation
            data_array[idx1:idx2] /= 1.4826 * np.ma.median(
               np.ma.abs(data_array[idx1:idx2]), axis=0
            )
            data_array_avg[idx1:idx2] /= 1.4826 * np.ma.median(
                np.ma.abs(data_array_avg[idx1:idx2]), axis=0
            )

        data_array = np.moveaxis(data_array, 0, 2)

        dnames = [
            "z-score",
            "flags",
            "z-score avg",
            "flags avg",
            "freq array",
            "dt",
            "time array",
            "ant pairs",
            "ntimes",
        ]
        data_list = [
            data_array,
            data_array.mask,
            data_array_avg,
            data_array_avg.mask,
            freq_array,
            dt,
            time_array,
            ant_pairs,
            ntimes,
        ]

        # save to hdf5 file
        print(f"\nsaving modified z-score: {path}")
        f = h5py.File(path, "a")

        for dname, data in zip(dnames, data_list):
            if dname in f.keys():
                del f[dname]
            f.create_dataset(dname, data=data)

        f.close()


def plot_time_series(
    time_array, data, ax=None, plot_masked=False, plotfile=None, **kwargs
):
    """Plot a time series

    Parameters
    ----------
    time_array : numpy array
        times
    data : numpy array
        time series data
    ax : matplotlib axes, optional
        plot to this matplotlib axis if provided, by default None
    plot_masked : bool, optional
        plot masked vaules, by default False
    plotfile : str, optional
        save plot to this file, by default None

    Returns
    -------
    matplotlib axes object

    """

    # plot without maskes
    if not plot_masked:
        data = data.data

    if isinstance(ax, type(None)):
        fig, ax = plt.subplots(figsize=(10, 5))

    ax.plot(time_array, data, color="k")
    ax.tick_params(labelrotation=90)
    ax.set_xlabel("Time [s]")
    ax.set_ylabel("Averaged Z-Score")

    if not isinstance(plotfile, type(None)):
        plt.savefig(plotfile)

    return ax


def plot_spec(
    freq_array,
    spec,
    ax=None,
    rfi_ranges=None,
    plot_protected=False,
    plot_internal=False,
    plot_masked=False,
    spw="*",
    plotfile=None,
):
    """Plot a spectrum

    Parameters
    ----------
    freq_array : numpy array
        frequencies
    spec : numpy array
        spectral data
    ax : matplotlib axes object, optional
        plot to this matplotlib axis if provided, by default None
    rfi_ranges : list of tuples, optional
        ranges known to be affected by RFI, by default None
    plot_protected : bool or list of tuples, optional
        plot protected frequencies, by default False
    plot_internal : bool or list of tuples, optional
        plot frequencies of VLA internal interference, by default False
    plot_masked : bool, optional
        plot masked values, by default False
    spw : str, optional
        spectral window to plot, by default "*"
    plotfile : str, optional
        save plot to this file, by default None

    Returns
    -------
    matplotlib axes object

    """
    ymax = 1e4

    # plot without maskes
    if not plot_masked:
        spec = spec.data

    if isinstance(ax, type(None)):
        fig, ax = plt.subplots(figsize=(10, 3))

    # plot rfi contaminated ranges
    if not isinstance(rfi_ranges, type(None)):
        for fmin, fmax in rfi_ranges:
            if fmin == fmax:
                lstyle = "-"
            else:
                lstyle = "--"
            ax.vlines(fmin, 0, ymax, color="red", linestyle=lstyle, linewidth=0.5)

            if fmin != fmax:
                ax.vlines(fmax, 0, ymax, color="red", linestyle=lstyle, linewidth=0.5)
                ax.fill_betweenx((0, ymax), fmin, fmax, color="red", alpha=0.05)

    # plot internal interference from the VLA
    if plot_internal:
        for freq in vla_internal:
            ax.vlines(freq, 0, ymax, color="orange", linestyle="-", linewidth=0.5)

    # plot protected frequency ranges
    if plot_protected:
        for fmin, fmax in protected_freq:
            ax.vlines(fmin, 0, ymax, color="blue", linestyle="--", linewidth=0.5)
            ax.vlines(fmax, 0, ymax, color="blue", linestyle="--", linewidth=0.5)
            ax.fill_betweenx((0, ymax), fmin, fmax, color="blue", alpha=0.05)

    # plot spectral windows
    for i in range(16):
        ax.vlines(
            freq_array[i * 64], 0, ymax, color="purple", linestyle="-", linewidth=1
        )
        ax.text(freq_array[i * 64 + 32], 2e1, f"{i}", ha="center")

    ax.plot(freq_array, spec, color="k", linewidth=1)
    ax.set_yscale("log")
    ax.set_ylim([1e-3, 1e1])
    ax.set_xlabel("Frequency [MHz]")
    ax.set_ylabel("Avg. Amplitudes (arbitrary units)")
    ax.minorticks_on()

    # plot whole band or just a spectral window?
    if isinstance(spw, int):
        ax.set_xlim([freq_array[spw * 64], freq_array[(spw + 1) * 64]])
    else:
        ax.set_xlim([np.min(freq_array), np.max(freq_array)])

    # plt.setp(ax, **kwargs)
    plt.tight_layout()

    if not isinstance(plotfile, type(None)):
        plt.savefig(plotfile)

    return ax


def plot_spec_spw_summary(
    freq_array,
    spec,
    ax=None,
    plotfile=None,
    plot_masked=False,
    **kwargs,
):
    """Plot spectal of all spectral windows in a summary plot

    Parameters
    ----------
    freq_array : numpy array
        frequencies
    spec : numpy array
        spectral data
    ax : matplotlib axes object, optional
        plot to this matplotlib axis if provided, by default None
    plot_masked : bool, optional
        plot masked values, by default False
    plotfile : str, optional
        save plot to this file, by default None

    Returns
    -------
    matplotlib axes object

    """
    ymax = 1e4

    # plot without maskes
    if not plot_masked:
        spec = spec.data

    if isinstance(ax, type(None)):
        fig, ax = plt.subplots(4, 4, figsize=(12, 10), sharex=True)

    chan = np.arange(64)

    for i in range(4):
        for j in range(4):
            idx_range = slice((i * 4 + j) * 64, (i * 4 + j + 1) * 64)
            fmin, fmax = int(np.min(freq_array[idx_range])), int(
                np.max(freq_array[idx_range])
            )
            ymin, ymax = np.min(spec[idx_range]), np.max(spec[idx_range][2:-2])
            dlogy = np.log10(ymax) - np.log10(ymin)
            ypos = 10 ** (np.log10(0.5 * ymin) + 0.05 * dlogy)
            ax[i, j].plot(chan, spec[idx_range], color="k", linewidth=1)
            ax[i, j].set_yscale("log")
            ax[i, j].text(
                32,
                ypos,
                rf"$\bf{{SPW\ {i * 4 + j}}}$: {fmin}-{fmax}$\,$MHz",
                verticalalignment="bottom",
                horizontalalignment="center",
                fontsize=10,
            )
            ax[i, j].set_xlim([0, 63])
            ax[i, j].set_ylim([0.5 * ymin, ymax * 2])
            ax[i, j].minorticks_on()
            # ax[i, j].set_ylim([1e-3, 1e2])

    plt.tight_layout()

    fig.subplots_adjust(bottom=0.1)
    fig.subplots_adjust(left=0.1)
    fig.text(0.5, 0.04, "Channel", ha="center", fontsize=12)
    fig.text(
        0.04,
        0.5,
        "Averaged msibility Amplitudes [uncalibrated Jy]",
        va="center",
        rotation="vertical",
        fontsize=12,
    )

    if not isinstance(plotfile, type(None)):
        plt.savefig(plotfile)

    return ax


def plot_spec_spw(freq_array, spec, spw, ax=None, fig=None, plot_masked=False):
    """Plot a spectrum

    Parameters
    ----------
    freq_array : numpy array
        frequencies
    spec : numpy array
        spectral data
    spw : str, optional
        spectral window to plot
    ax : matplotlib axes object, optional
        plot to this matplotlib axis if provided, by default None
    fig : matplotlib fig object, optional
        by default None
    plot_masked : bool, optional
        plot masked values, by default False

    Returns
    -------
    matplotlib axes object

    """

    # plot without maskes
    if not plot_masked:
        spec = spec.data

    if isinstance(ax, type(None)):
        fig, ax = plt.subplots(figsize=(12, 10))

    chan = np.arange(64)

    idx_range = slice(spw * 64, (spw + 1) * 64)
    fmin, fmax = int(np.min(freq_array[idx_range])), int(np.max(freq_array[idx_range]))
    ymin, ymax = np.min(spec[idx_range]), np.max(spec[idx_range][2:-2])

    ax.plot(chan, spec[idx_range], color="k", linewidth=2)
    ax.set_yscale("log")
    ax.set_title(
        rf"$\bf{{SPW\ {spw}}}$: {fmin}-{fmax}$\,$MHz",
        verticalalignment="bottom",
        horizontalalignment="center",
        fontsize=10,
    )
    ax.set_xlim([0, 63])
    ax.set_ylim([0.5 * ymin, ymax * 2])
    ax.set_xticks(np.arange(0, 64, 5))
    ax.minorticks_on()
    ax.grid(which="major", linewidth=1)
    ax.grid(which="minor", linewidth=0.5, alpha=0.5)

    plt.tight_layout()

    fig.subplots_adjust(bottom=0.1)
    fig.subplots_adjust(left=0.1)
    fig.text(0.5, 0.04, "Channel", ha="center", fontsize=12)
    fig.text(
        0.04,
        0.5,
        "Averaged msibility Amplitudes [uncalibrated Jy]",
        va="center",
        rotation="vertical",
        fontsize=12,
    )

    return ax


def plot_wf(
    freq_array,
    wf,
    dt=1.0,
    scan_boundaries=None,
    ax=None,
    spw="*",
    plot_masked=False,
    plotfile=None,
    **kwargs,
):
    """Plot spectrogram (waterfall plot)

    Parameters
    ----------
    freq_array : numpy array
        array of frequencies
    wf : numpy array
        spectrogram data (waterfall)
    dt : float, optional
        time integration in seconds, by default 1
    scan_boundaries : list, optional
        scan boundaries, by default None
    ax : matplotlib axes object, optional
        plot to this matplotlib axis if provided, by default None
    spw : str, optional
        spectral window to plot, by default "*"
    plot_masked : bool, optional
        if true, plot masked values by default False
    plotfile : str, optional
        save plot to this file, by default None

    Returns
    -------
    matplotlib axes object

    """

    # plot without maskes
    if not plot_masked:
        wf = wf.data

    if isinstance(ax, type(None)):
        fig, ax = plt.subplots(figsize=(10, 10))

    im = ax.imshow(
        wf,
        interpolation="nearest",
        aspect="auto",
        extent=(1008, 2031, dt * wf.shape[0], 0),
        **kwargs,
    )
    ax.set_xlabel("Frequency [MHz]")
    ax.set_ylabel("Time [s]")
    ax.minorticks_on()

    # plot whole band or just a spectral window?
    if isinstance(spw, int):
        ax.set_xlim([freq_array[spw * 64], freq_array[(spw + 1) * 64]])
    else:
        ax.set_xlim([np.min(freq_array), np.max(freq_array)])

    # plot spectral window boundaries
    for i in range(16):
        ax.vlines(
            freq_array[i * 64],
            0,
            dt * wf.shape[0],
            color="white",
            linestyle="dashed",
            linewidth=1,
        )
        ax.text(freq_array[i * 64 + 32], -10, f"{i}", ha="center")

    # plot scan boundaries
    if not isinstance(scan_boundaries, type(None)):
        for i in range(len(scan_boundaries)):
            idx1 = int(np.sum(scan_boundaries[:i]))
            idx2 = int(np.sum(scan_boundaries[: i + 1]))
            ax.hlines(2 * idx2, 1008, 2031, color="white", linestyle="-", linewidth=2)
            ax.text(2040, idx1 + idx2, f"Scan {i+1}", va="center")

    plt.tight_layout()

    if not isinstance(plotfile, type(None)):
        plt.savefig(plotfile)

    return ax, im


def plot_wf_spw(
    freq_array,
    wf,
    spw,
    dt=1,
    scan_boundaries=None,
    ax=None,
    plot_masked=False,
    **kwargs,
):
    """Plot spectrogram (waterfall plot) for one spectral window

    Parameters
    ----------
    freq_array : numpy array
        array of frequencies
    wf : numpy array
        spectrogram data (waterfall)
    spw : int
        spectral window
    dt : float, optional
        time integration in seconds, by default 1
    scan_boundaries : list, optional
        scan boundaries, by default None
    ax : matplotlib axes object, optional
        plot to this matplotlib axis if provided, by default None
    plot_masked : bool, optional
        if true, plot masked values by default False

    Returns
    -------
    matplotlib axes object

    """

    # plot without maskes
    if not plot_masked:
        wf = wf.data

    if isinstance(ax, type(None)):
        fig, ax = plt.subplots(figsize=(12, 10))

    chan = np.arange(64)

    idx_range = slice(spw * 64, (spw + 1) * 64)
    fmin, fmax = int(np.min(freq_array[idx_range])), int(np.max(freq_array[idx_range]))
    ax.imshow(
        wf[:, idx_range],
        interpolation="nearest",
        aspect="auto",
        vmin=0.0,
        vmax=2.0,
        extent=(0, 64, dt * wf.shape[0], 0),
        **kwargs,
    )
    ax.set_xlabel("Channel")
    ax.set_ylabel("Time [s]")
    ax.set_xticks(np.arange(0, 64, 5))
    ax.minorticks_on()
    ax.grid(which="major", linewidth=1)
    ax.grid(which="minor", linewidth=0.5, alpha=0.5)
    ax.set_title(
        rf"$\bf{{SPW\ {spw}}}$: {fmin}-{fmax}$\,$MHz",
        verticalalignment="bottom",
        horizontalalignment="center",
        fontsize=10,
    )

    # plot scan boundaries
    if not isinstance(scan_boundaries, type(None)):
        for i in range(8):
            idx1 = int(np.sum(scan_boundaries[:i]))
            idx2 = int(np.sum(scan_boundaries[: i + 1]))
            ax.hlines(2 * idx2, 0, 64, color="white", linestyle="-", linewidth=2)
            ax.text(65, idx1 + idx2, f"Scan {i+1}", va="center")

    plt.tight_layout()

    return ax
