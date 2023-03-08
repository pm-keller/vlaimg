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
import glob
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


def selfcal(ms, rnd=0, calmode="p", solint="200s", overwrite=False):
    """self calibration

    Parameters
    ----------
    ms : str
        path to measurement set
    rnd : int, optional
        round, by default 0
    calmode : str, optional
        calibration mode (phase "p" or amplitude-phase "ap"), by default "p"
    solint : str, optional
        solution interval, by default "200s"
    overwrite : bool, optional
        if true, overwrite existing calibration tables, by default False

    Returns
    -------
    str
        path to self-calibrated measurement set
    """

    print(f"\nself-calibration round {rnd+1}, calibration mode: {calmode}")

    field = ms.split("/")[-1].split(".")[-2]
    root = os.path.dirname(ms)

    caltable = os.path.join(root, f"caltables/{field}.selfcal_{rnd+1}.{calmode}.G")
    ms_selfcal = os.path.join(root, f"{field}.selfcal_{rnd+1}.ms")

    casatasks.gaincal(
        ms,
        caltable=caltable,
        solint=solint,
        calmode=calmode,
        solnorm=True,
        refant="ea10",
        gaintype="G",
        minsnr=5,
    )

    casatasks.applycal(
        ms,
        gaintable=[
            caltable,
        ],
        interp="linear",
    )

    if os.path.exists(ms_selfcal) and overwrite:
        shutil.rmtree(ms_selfcal)
        fnames = glob.glob(ms_selfcal[:-2] + "im*")

        for fname in fnames:
            shutil.rmtree(fname)

    casatasks.split(ms, outputvis=ms_selfcal, datacolumn="corrected")

    return ms_selfcal


def tclean(ms, spw, nsigma, uvtaper="50klambda"):
    """run CASA's tclean imaging algorithm

    Parameters
    ----------
    ms : str
        path to measurement set
    spw : str
        spectral windows
    nsigma : int
        stopping threshold
    uvtaper : str, optional
        size of Gaussian UV-taper, by default "50klambda"
    """

    im = ms[-2] + "im"

    casatasks.tclean(
        ms,
        imagename=im,
        spw=spw,
        cell="0.3arcsec",
        imsize=10000,
        niter=10000,
        gain=0.2,
        pblimit=-0.1,
        nsigma=nsigma,
        uvtaper=uvtaper,
        specmode="mfs",
        deconvolver="mtmfs",
        nterms=2,
        weighting="briggs",
        stokes="I",
        savemodel="modelcolumn",
        pbcor=True,
        mask="",
        robust=-0.5,
        interactive=False,
        parallel=True,
    )
