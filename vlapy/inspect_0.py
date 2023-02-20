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

import numpy as np

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

import sys

sys.path.append(
    "/DATA/CARINA_3/kel334/19A-056/vlaimg"
)

from vlapy import vladata
from vlapy.rfi_freq import (
    protected_freq,
    vla_internal,
)


def get_data_products(
    vis, avg_ntimes=None, bls_bins=None, masked=True, data_column="DATA"
):
    """
    Get data products needed for the inspection.
    """

    # get data array from data file
    uvd = vladata.get_uvdata(vis)
    data_array = vladata.get_data_array(uvd, data_column)

    if masked:
        flags = vladata.get_flag_array(uvd, data_column)
        data_array = np.ma.masked_array(data_array, mask=flags)

    # get header data from file
    freq_array = uvd.freq_array[0] * 1e-6
    dt = uvd.integration_time[0]
    time_array = uvd.time_array
    ant_pairs = uvd.get_antpairs()

    # average across polarisation products and baselines to obtain waterfall data time vs. frequency
    rfi_wf = np.ma.mean(np.ma.abs(data_array), axis=(0, 1))

    # average waterfall across time to obtain a single frequency spectrum
    rfi_spec = np.ma.mean(rfi_wf, axis=0)

    # get antenna-based averaged waterfalls
    per_ant_wf = []
    for ant in uvd.antenna_numbers:
        idx = [i for i, pair in enumerate(ant_pairs) if ant in pair]
        per_ant_wf.append(np.ma.mean(np.ma.abs(data_array[:, idx]), axis=(0, 1)))

    per_ant_wf = np.ma.array(per_ant_wf)
    per_ant_spec = np.ma.mean(per_ant_wf, axis=1)

    # get normalised waterfalls averaged across baselines in bins of baselines lengths
    bls_lens = []
    for pair in ant_pairs:
        idx1 = np.where(pair[0] == uvd.antenna_numbers)[0]
        idx2 = np.where(pair[1] == uvd.antenna_numbers)[0]
        pos1 = uvd.antenna_positions[idx1]
        pos2 = uvd.antenna_positions[idx2]
        bls_lens.append(np.sqrt(np.sum((pos2 - pos1) ** 2, axis=-1)))

    bls_lens = np.ma.array(bls_lens)
    bls_bin_wf = []
    for i in range(len(bls_bins) - 1):
        idx = np.where((bls_lens > bls_bins[i]) & (bls_lens < bls_bins[i + 1]))[0]
        bls_bin_wf.append(np.ma.mean(np.ma.abs(data_array[:, idx]), axis=(0, 1)))

    bls_bin_wf = np.ma.array(bls_bin_wf)
    bls_bin_spec = np.ma.mean(bls_bin_wf, axis=1)

    # normalise waterfalls by median spectrum across scans
    per_ant_wf = np.moveaxis(per_ant_wf, 1, 0)
    bls_bin_wf = np.moveaxis(bls_bin_wf, 1, 0)

    if isinstance(avg_ntimes, type(None)):
        rfi_wf_norm -= np.ma.median(rfi_wf, axis=0)
        per_ant_wf -= np.ma.median(per_ant_wf, axis=0)
        bls_bin_wf -= np.ma.median(bls_bin_wf, axis=0)

        rfi_wf_norm /= np.ma.median(1.4826 * np.abs(rfi_wf), axis=0)
        per_ant_wf /= np.ma.median(1.4826 * np.abs(per_ant_wf), axis=0)
        bls_bin_wf /= np.ma.median(1.4826 * np.abs(bls_bin_wf), axis=0)

    else:
        rfi_wf_norm = rfi_wf.copy()

        for i in range(len(avg_ntimes)):
            idx1 = int(np.sum(avg_ntimes[:i]))
            idx2 = int(np.sum(avg_ntimes[: i + 1]))

            rfi_wf_norm[idx1:idx2] -= np.ma.median(rfi_wf[idx1:idx2], axis=0)
            per_ant_wf[idx1:idx2] -= np.ma.median(per_ant_wf[idx1:idx2], axis=0)
            bls_bin_wf[idx1:idx2] -= np.ma.median(bls_bin_wf[idx1:idx2], axis=0)

            rfi_wf_norm[idx1:idx2] /= np.ma.median(
                1.4826 * np.abs(rfi_wf_norm[idx1:idx2]), axis=0
            )
            per_ant_wf[idx1:idx2] /= np.ma.median(
                1.4826 * np.abs(per_ant_wf[idx1:idx2]), axis=0
            )
            bls_bin_wf[idx1:idx2] /= np.ma.median(
                1.4826 * np.abs(bls_bin_wf[idx1:idx2]), axis=0
            )

    per_ant_wf = np.moveaxis(per_ant_wf, 0, 1)
    bls_bin_wf = np.moveaxis(bls_bin_wf, 0, 1)

    return {
        "data array": data_array,
        "freq array": freq_array,
        "time array": time_array,
        "dt": dt,
        "ant pairs": ant_pairs,
        "ant nums": uvd.antenna_numbers,
        "ant names": uvd.antenna_names,
        "rfi wf": rfi_wf,
        "rfi spec": rfi_spec,
        "rfi wf norm": rfi_wf_norm,
        "per ant wf": per_ant_wf,
        "per ant spec": per_ant_spec,
        "bls bin wf": bls_bin_wf,
        "bls bin spec": bls_bin_spec,
    }


def plot_time_series(
    time_array, data, ax=None, plot_masked=False, plotfile=None, **kwargs
):
    """
    Plot a time series
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
    **kwargs,
):
    """
    Plot a spectrum
    """
    ymax = 1e4

    # plot without maskes
    if not plot_masked:
        spec = spec.data

    if isinstance(ax, type(None)):
        fig, ax = plt.subplots(figsize=(10, 3))

    # plot rfi contaminated ranges
    if not isinstance(rfi_ranges, type(None)):
        for (fmin, fmax) in rfi_ranges:
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
        for (fmin, fmax) in protected_freq:
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
    """
    Plot summary plot of spectral window spectra
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
        "Averaged Visibility Amplitudes [uncalibrated Jy]",
        va="center",
        rotation="vertical",
        fontsize=12,
    )

    if not isinstance(plotfile, type(None)):
        plt.savefig(plotfile)

    return ax


def plot_spec_spw(freq_array, spec, spw, ax=None, fig=None, plot_masked=False):
    """
    Plot spectral windows
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
        "Averaged Visibility Amplitudes [uncalibrated Jy]",
        va="center",
        rotation="vertical",
        fontsize=12,
    )

    return ax


def plot_wf(
    freq_array,
    wf,
    dt=1,
    scan_boundaries=None,
    ax=None,
    spw="*",
    plot_masked=False,
    plotfile=None,
    **kwargs,
):
    """
    Plot Spectrogram (Waterfall plot)
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
        for i in range(8):
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
    """
    Plot spectral windows spectrograms
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
        vmax=3.0,
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
