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
import sys
import copy
import shutil
import casatools
import casatasks
import numpy as np

from prefect import task
from prefect.tasks import task_input_hash

sys.path.append(os.getcwd())
from vlapy import vladata, flagging


# @task(cache_key_fn=task_input_hash)
def setjy(ms, overwrite=False):
    """Calculate and set a calibrator model.

    Parameters
    ----------
    ms : str
        path to measurement set
    overwrite : bool, optional
        if true, overwrite existing model, by default False

    Returns
    -------
    list of str
        summary file

    """

    root = os.path.dirname(ms)
    printfile = os.path.join(root, "output/setjy.npy")

    if (not os.path.exists) or overwrite:
        field_dict = vladata.get_field_names(ms)
        field = field_dict["fluxcal"]
        model = field_dict["model"]

        print(f"\ncalculating and setting calibration model {model}\n")
        setjy = casatasks.setjy(ms, model=model, field=field, usescratch=True)

        print(f"saving summary: {printfile}")
        np.save(printfile, setjy)

    return printfile


# @task(cache_key_fn=task_input_hash)
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
        calibration tables
    """

    # root directory
    root = os.path.dirname(ms)

    # specify calibration table names
    opacity_table = os.path.join(root, f"caltables/{name}.opac")
    rq_table = os.path.join(root, f"caltables/{name}.rq")
    swpow_table = os.path.join(root, f"caltables/{name}.swpow")
    antpos_table = os.path.join(root, f"caltables/{name}.antpos")

    plotfile = os.path.join(root, "plots/calplots/weather.png")
    printfile = os.path.join(root, "output/weather.npy")

    if (not os.path.exists(plotfile)) and (not os.path.exists(printfile)):
        print(f"\nplot weather: {plotfile}")

        weather = casatasks.plotweather(
            ms,
            seasonal_weight=0.5,
            doPlot=True,
            plotName=plotfile,
        )

        print(f"\nsaving opacities: {printfile}")
        np.save(printfile, weather)

    if not os.path.exists(opacity_table):
        print(f"\nopacity corrections: {opacity_table}")
        casatasks.gencal(
            ms,
            caltable=opacity_table,
            caltype="opac",
            spw="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15",
            parameter=weather,
        )

    if not os.path.exists(rq_table):
        print(f"\nrequantizer gains: {rq_table}")
        casatasks.gencal(ms, caltable=rq_table, caltype="rq")

    if not os.path.exists(swpow_table):
        print(f"\nEVLA switched power gains: {swpow_table}")
        casatasks.gencal(
            ms,
            swpow_table,
            caltype="swpow",
        )

    if not os.path.exists(antpos_table):
        print(f"\nantenna position corrections: {antpos_table}")
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

    return [opacity_table, rq_table, antpos_table]


# @task(cache_key_fn=task_input_hash)
def initcal(
    ms, name, refant, calchan, priortables, rnd=0, calmode="ap", overwrite=False
):
    """Delay and bandpass calibration.
    Also use this as an initial calibration for subsequent automated RFI excision.

    Parameters
    ----------
    ms : str
        path to measurement set
    name : str
        data name
    refant : str
        reference antenna
    calchan : str
        calibration channels
    priortables : list of str
        prior calibration tables
    rnd : int
        calibration round
    calmode : str
        calibration mode of initial bandpass gain calibration ("p" or "ap"), by default "ap"
    overwrite : bool
        if true, overwrite existing calibration tables, by default False

    Returns
    -------
    list of str
        calibration tables
    """

    root = os.path.dirname(ms)

    # get field names
    field_dict = vladata.get_field_names(ms)
    field = field_dict["fluxcal"]

    flagging.save(ms, f"initcal_round_{rnd}", "before", field)

    # specify gain tables
    gaintables = copy.deepcopy(priortables)

    delay_init_table = os.path.join(root, f"caltables/{name}.p.G{rnd}")
    delay_table = os.path.join(root, f"caltables/{name}.K{rnd}")
    bandpass_init_table = os.path.join(root, f"caltables/{name}.ap.G{rnd}")
    bandpass_table = os.path.join(root, f"caltables/{name}.B{rnd}")

    # remove calibration files
    if overwrite:
        for table in [
            delay_init_table,
            delay_table,
            bandpass_init_table,
            bandpass_table,
        ]:
            print(f"\nremoving calibration table {table}")
            if os.path.exists(table):
                shutil.rmtree(table)

    # derive calibration tables
    if (not os.path.exists(delay_init_table)) or overwrite:
        print(f"\ndelay initial phase calibration: {delay_init_table}")
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

    if (not os.path.exists(delay_table)) or overwrite:
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

    if (not os.path.exists(bandpass_init_table)) or overwrite:
        print("\nbandpass initial gain calibration")
        casatasks.gaincal(
            ms,
            field=field,
            refant=refant,
            spw=calchan,
            gaintype="G",
            calmode=calmode,
            solint="int",
            minsnr=5.0,
            caltable=bandpass_init_table,
            gaintable=gaintables,
            parang=True,
        )
    gaintables.append(bandpass_init_table)

    if (not os.path.exists(bandpass_table)) or overwrite:
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

    if calmode == "ap":
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

    gaintables.append(bandpass_table)

    print(f"\napply calibration: {gaintables}")
    casatasks.applycal(
        ms,
        gaintable=gaintables,
        interp=["", "", "", "", "", "nearest,nearestflag"],
        calwt=False,
        parang=True,
        applymode="calflagstrict",
        flagbackup=False,
    )

    flagging.save(ms, f"initcal_round_{rnd}", "after", field)

    return [delay_init_table, delay_table, bandpass_init_table, bandpass_table]


# @task(cache_key_fn=task_input_hash)
def fluxboot(ms, name, chans, refant, solint_max, overwrite=False):
    """Flux bootstrapping

    Parameters
    ----------
    ms : str
        path to measurement set
    name : str
        data name
    chans : str
        calibration channels
    refant : str
        reference antenna
    solint_max : str
        maximum solution interval
    overwrite : bool, optional
        if true, overwrite existing fluxscale, by default False

    Returns
    -------
    list
        fluxscale tables
    """

    print("\nFLUX BOOTSTRAPPING")

    root = os.path.dirname(ms)
    ms_calibrators = root + "/calibrators.ms"

    # get field and model
    field_dict = vladata.get_field_names(ms)
    model = field_dict["model"]
    fluxcal = field_dict["fluxcal"]
    calibrators = field_dict["calibrators"]

    fluxscale_list = []

    # specify calibration tables
    short_gain_table = root + f"/caltables/{name}_short.G"
    long_gain_table = root + f"/caltables/{name}_long.G"
    flux_gain_table = root + f"/caltables/{name}_fluxgain.G"

    # remove calibrator measurement sets if they exist and overwrite=True
    if os.path.exists(ms_calibrators) and overwrite:
        print(f"\nremoving {ms_calibrators}")
        shutil.rmtree(ms_calibrators)

    if os.path.exists(ms_calibrators + ".flagversions") and overwrite:
        print(f"\nremoving {ms_calibrators}.flagversions")
        shutil.rmtree(ms_calibrators + ".flagversions")

    if (not os.path.exists(ms_calibrators)) or overwrite:
        print(f"\nsplitting calibrators: {ms_calibrators}")
        casatasks.split(
            vis=ms,
            outputvis=ms_calibrators,
            field=calibrators,
            keepmms=True,
            datacolumn="corrected",
            keepflags=False,
        )

        print("\nset model flux density")
        casatasks.setjy(
            vis=ms_calibrators,
            field=fluxcal,
            model=model,
            usescratch=True,
        )
        casatasks.setjy(
            vis=ms,
            field=fluxcal,
            model=model,
            usescratch=True,
        )

        printfile = root + "/output/fluxboot_summary_before.npy"

        flagging.save(ms_calibrators, "fluxboot", "before", calibrators)

        # remove calibration files
        for filename in [long_gain_table, short_gain_table, flux_gain_table]:
            if os.path.exists(filename):
                shutil.rmtree(filename)

        for j, calibrator in enumerate(calibrators.split(",")):
            print(f"\ngain calibration on field {calibrator}")

            if j == 0:
                append = False
            else:
                append = True

            # initial gain calibration on all calibrators using a short interval
            # to avoid decorrelation on the long interval
            casatasks.gaincal(
                ms_calibrators,
                caltable=short_gain_table,
                field=calibrator,
                spw=chans,
                solint="int",
                refant=refant,
                gaintype="G",
                calmode="p",
                minsnr=5.0,
                parang=True,
                append=append,
            )

            # gain calibration on all calibrators using the long interval (scan)
            # this calibration table will be used for flagging bad solutions
            casatasks.gaincal(
                ms_calibrators,
                caltable=long_gain_table,
                field=calibrator,
                spw=chans,
                solint=solint_max,
                refant=refant,
                solnorm=True,
                gaintype="G",
                calmode="ap",
                minsnr=5.0,
                append=append,
                gaintable=[short_gain_table],
            )

        print("\nflag gains")
        casatasks.flagdata(
            long_gain_table,
            field=calibrator,
            mode="clip",
            correlation="ABS_ALL",
            clipminmax=[0.9, 1.1],
            datacolumn="CPARAM",
            clipoutside=True,
            action="apply",
            flagbackup=False,
        )

        print("\napply flags")
        casatasks.applycal(
            ms_calibrators,
            gaintable=[
                long_gain_table,
            ],
            calwt=[False],
            applymode="flagonlystrict",
            flagbackup=True,
        )

        for j, calibrator in enumerate(calibrators.split(",")):
            print(f"\nflux bootstrapping field {calibrator}")

            if j == 0:
                append = False
            else:
                append = True

            # gain calibration for subsequent spectral index fitting
            casatasks.gaincal(
                vis=ms_calibrators,
                caltable=flux_gain_table,
                field=calibrator,
                spw=chans,
                solint=solint_max,
                refant=refant,
                minsnr=5.0,
                solnorm=False,
                gaintype="G",
                calmode="ap",
                append=append,
                gaintable=[short_gain_table],
                parang=True,
            )

            if j > 0:
                if os.path.exists(f"{name}.fluxscale{j}"):
                    shutil.rmtree(f"{name}.fluxscale{j}")

                # transfer flux scale from primary calibrator to phase calibrators
                # fit flux and sprectral index to the phase calibrators
                fluxtable = root + f"/output/{name}.fluxscale{j}"

                # remove existing flux table
                if os.path.exists(fluxtable) and overwrite:
                    shutil.rmtree(fluxtable)

                if not os.path.exists(fluxtable):
                    fluxscale = casatasks.fluxscale(
                        ms_calibrators,
                        caltable=flux_gain_table,
                        fluxtable=fluxtable,
                        reference=[fluxcal],
                        transfer=[calibrator],
                        fitorder=2,
                    )

                id = [key for key in fluxscale.keys()][0]

                # set and calculate model of phase calibrators
                casatasks.setjy(
                    ms_calibrators,
                    field=calibrator,
                    fluxdensity=fluxscale[id]["fitFluxd"],
                    spix=fluxscale[id]["spidx"],
                    reffreq=str(fluxscale[id]["fitRefFreq"]) + "Hz",
                    usescratch=True,
                    standard="manual",
                )
                casatasks.setjy(
                    ms,
                    field=calibrator,
                    fluxdensity=fluxscale[id]["fitFluxd"],
                    spix=fluxscale[id]["spidx"],
                    reffreq=str(fluxscale[id]["fitRefFreq"]) + "Hz",
                    usescratch=True,
                    standard="manual",
                )

                printfile = root + f"/output/phasecal_model_fit_{j}.npy"
                print(f"\nsaving phase calibrator model fits: {printfile}")
                np.save(printfile, fluxscale)
                fluxscale_list.append(fluxscale)

        flagging.save(ms_calibrators, "fluxboot", "after", calibrators)

    return (fluxscale_list, [short_gain_table, long_gain_table, flux_gain_table])


# @task(cache_key_fn=task_input_hash)
def finalcal(ms, name, refant, calchan, solint_max, gaintables, overwrite=False):
    """Final phase and amplitude calibration

    Parameters
    ----------
    ms : str
        path to measurement set
    name : str
        data name
    refant : str
        reference antenna
    calchan : str
        calibration channels
    solint_max : str
        maximum solution interval for gaincalibration
    gaintables : list of str
        priortables, delay and bandpass tables
    overwrite : bool, optional
        if true, overwrite existing calibration tables, by default False

    Returns
    -------
    list of str
        calibration tables
    """

    root = os.path.dirname(ms)
    ms_calibrators = os.path.join(root, "finalcalibrators.ms")

    # get field and model
    field_dict = vladata.get_field_names(ms)
    fluxcal = field_dict["fluxcal"]
    calibrators = field_dict["calibrators"]
    model = field_dict["model"]

    # save flag status
    flagging.save(ms, "finalcal", "before", calibrators)

    # specify gain tables
    fluxcal_phase_table = root + f"/caltables/{name}_fluxcal_phase.Gfinal"
    short_gain_table = root + f"/caltables/{name}_short.Gfinal"
    amp_gain_table = root + f"/caltables/{name}_amp.Gfinal"
    phase_gain_table = root + f"/caltables/{name}_phase.Gfinal"

    finaltables = copy.deepcopy(gaintables)

    # remove calibration files
    if overwrite:
        for table in [
            fluxcal_phase_table,
            short_gain_table,
            amp_gain_table,
            phase_gain_table,
        ]:
            if os.path.exists(table):
                print(f"\nremoving calibration table {table}")
                shutil.rmtree(table)

    if (not os.path.exists(fluxcal_phase_table)) or overwrite:
        print(f"\naveraged phase calibration on flux calibrator: {fluxcal_phase_table}")
        casatasks.gaincal(
            ms,
            caltable=fluxcal_phase_table,
            field=fluxcal,
            spw=calchan,
            selectdata=True,
            solint="inf",
            refant=refant,
            minsnr=1.0,
            solnorm=False,
            gaintype="G",
            calmode="p",
            gaintable=finaltables,
            parang=True,
            append=False,
        )

    # remove any flags on this gaintable
    casatasks.flagdata(
        fluxcal_phase_table,
        mode="unflag",
        action="apply",
        flagbackup=False,
        savepars=False,
    )

    print("\napply calibration")
    finaltables.append(fluxcal_phase_table)
    casatasks.applycal(
        ms,
        gaintable=finaltables,
        calwt=False,
        parang=True,
        applymode="calflagstrict",
        flagbackup=True,
    )

    print(f"\nsplit calibrators: {ms_calibrators}")
    if os.path.exists(ms_calibrators) and overwrite:
        shutil.rmtree(ms_calibrators)
    if (not os.path.exists(ms_calibrators)) or overwrite:
        casatasks.split(
            ms,
            outputvis=ms_calibrators,
            field=calibrators,
            datacolumn="corrected",
            keepflags=False,
        )

    for i, calibrator in enumerate(calibrators.split(",")):
        print(f"\ncalibrator: {calibrator}")
        print(f"\ncalculate and set flux models")

        if i == 0:
            casatasks.setjy(
                ms_calibrators,
                field=fluxcal,
                model=model,
                usescratch=True,
            )

            append = False
        else:
            fit = np.load(
                root + f"/output/phasecal_model_fit_{i}.npy", allow_pickle=True
            ).item()

            id = [key for key in fit.keys()][0]
            fit = fit[id]

            casatasks.setjy(
                ms_calibrators,
                field=calibrator,
                standard="manual",
                fluxdensity=fit["fitFluxd"],
                spix=fit["spidx"],
                reffreq=str(fit["fitRefFreq"]) + "Hz",
                usescratch=True,
            )

            append = True

        if (not os.path.exists(short_gain_table)) or overwrite:
            print(f"\nshort gain calibration: {short_gain_table}")
            print(calibrator, append)
            casatasks.gaincal(
                ms_calibrators,
                caltable=short_gain_table,
                field=calibrator,
                solint="int",
                refant=refant,
                minsnr=5.0,
                gaintype="G",
                calmode="p",
                append=append,
                parang=True,
            )

        finaltables = [
            short_gain_table,
        ]

        if (not os.path.exists(amp_gain_table)) or overwrite:
            print(f"\namplitude gain calibration: {amp_gain_table}")
            casatasks.gaincal(
                ms_calibrators,
                caltable=amp_gain_table,
                field=calibrator,
                solint=solint_max,
                refant=refant,
                minsnr=5.0,
                gaintype="G",
                calmode="ap",
                gaintable=finaltables,
                append=append,
                parang=True,
            )

        finaltables.append(amp_gain_table)
        finaltables.remove(short_gain_table)

        if (not os.path.exists(phase_gain_table)) or overwrite:
            print(f"\nphase gain calibration: {phase_gain_table}")
            casatasks.gaincal(
                ms_calibrators,
                caltable=phase_gain_table,
                field=calibrator,
                solint=solint_max,
                refant=refant,
                minsnr=5.0,
                gaintype="G",
                calmode="p",
                gaintable=finaltables,
                append=append,
                parang=True,
            )

    return (fluxcal_phase_table, short_gain_table, amp_gain_table, phase_gain_table)


# @task(cache_key_fn=task_input_hash)
def apply(ms, gaintables):
    """Apply calibration tables to data

    Parameters
    ----------
    ms : str
        path to measurement set
    gaintables : list of str
        priortables, delay and bandpass, gain amplitude and phase tables
    """

    root = os.path.dirname(ms)

    printfile = os.path.join(root, "output/applycal_summary_before.npy")

    print(f"\nflagging summary: {printfile}")
    summary_1 = casatasks.flagdata(ms, mode="summary")
    np.save(printfile, summary_1)

    print(f"\napply calibration: {gaintables}")
    casatasks.applycal(
        ms,
        gaintable=gaintables,
        interp=["", "", "", "", "", "nearest,nearestflag", "", "", ""],
        calwt=False,
        parang=True,
        applymode="calflagstrict",
        flagbackup=False,
    )

    printfile = os.path.join(root, "output/applycal_summary_after.npy")

    print(f"\nflagging summary: {printfile}")
    summary_2 = casatasks.flagdata(ms, mode="summary")
    np.save(printfile, summary_2)

    printfile = os.path.join(root, "output/applycal_summary_detailed.npy")

    print(f"\nflagging summary: {printfile}")
    summary_3 = casatasks.flagdata(
        ms,
        mode="list",
        inpfile=[
            "spw='0' fieldcnt=True mode='summary' name='AntSpw000",
            "spw='1' fieldcnt=True mode='summary' name='AntSpw001",
            "spw='2' fieldcnt=True mode='summary' name='AntSpw002",
            "spw='3' fieldcnt=True mode='summary' name='AntSpw003",
            "spw='4' fieldcnt=True mode='summary' name='AntSpw004",
            "spw='5' fieldcnt=True mode='summary' name='AntSpw005",
            "spw='6' fieldcnt=True mode='summary' name='AntSpw006",
            "spw='7' fieldcnt=True mode='summary' name='AntSpw007",
            "spw='8' fieldcnt=True mode='summary' name='AntSpw008",
            "spw='9' fieldcnt=True mode='summary' name='AntSpw009",
            "spw='10' fieldcnt=True mode='summary' name='AntSpw010",
            "spw='11' fieldcnt=True mode='summary' name='AntSpw011",
            "spw='12' fieldcnt=True mode='summary' name='AntSpw012",
            "spw='13' fieldcnt=True mode='summary' name='AntSpw013",
            "spw='14' fieldcnt=True mode='summary' name='AntSpw014",
            "spw='15' fieldcnt=True mode='summary' name='AntSpw015",
        ],
        flagbackup=False,
    )

    np.save(printfile, summary_3)
