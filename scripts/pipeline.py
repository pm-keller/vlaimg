import os
import sys
import yaml
import casatools
import casatasks
import numpy as np

# from prefect import flow

sys.path.append(os.getcwd())
from vlapy import vladata, plot, flagging, calibration, inspect, imaging
from vlapy.hanning import hanning

import matplotlib

matplotlib.use("agg")

# read yaml configuration file
with open("input/config.yaml", "r") as file:
    conf = yaml.safe_load(file)


# @flow
def pipeline_1(obs):
    """
    The first part of the VLA data processing pipeline.
    This includes all the apriori data operations.
    Before proceeding to the next part of the data processing pipeline,
    the data needs to be inspected visually.
    """

    print(f"\nprocessing observation {obs}")

    # path to measurement set
    ms = os.path.join(conf["root"], obs, obs + ".ms")
    name = obs.split(".")[0]
    root = os.path.dirname(ms)

    # get field names
    field_dict = vladata.get_field_names(ms)
    fluxcal = field_dict["fluxcal"]
    targets = field_dict["targets"]
    phasecal = field_dict["phasecal"]
    calibrators = field_dict["calibrators"]

    # get index of observation
    idx = np.where(np.array(conf["obs list"]) == obs)[0][0]

    # make data directories
    vladata.makedir(conf["root"], obs)

    # hanning smoothing
    ms_hanning = hanning(ms, overwrite=False)

    # make observation plots
    plot.plotobs(ms_hanning, overwrite=False)

    # VLA deterministic flags
    flagging.detflags(
        ms_hanning,
        conf["flagging"]["quack"],
        conf["flagging"]["spw edge chan"],
        conf["spw"],
        conf["flagging"]["apriori"],
        reapply=False,
    )

    # compute modified z-score
    inspect.get_mod_z_score_data(
        ms_hanning, masked=True, data_column="DATA", overwrite=False
    )

    # VLA prior calibration
    priortables = calibration.priorcal(ms_hanning, name)

    # compute and plot primary calibrator model
    calibration.setjy(ms_hanning, overwrite=True)
    plot.setjy_model_amp_vs_uvdist(ms_hanning, overwrite=False)

    # plot amplitude vs. frequency to find dead antennas
    plot.find_dead_ants_amp_vs_freq(ms_hanning, overwrite=False)

    # plot calibration channels amplitude vs. time
    plot.single_chans_amp_vs_time(ms_hanning, conf["cal chans"][idx], overwrite=False)

    # perform initial calibration round 0
    gaintables = calibration.initcal(
        ms_hanning,
        name,
        conf["refant"][idx],
        conf["cal chans"][idx],
        priortables,
        rnd=0,
        overwrite=False,
    )

    # plot initial calibration
    plot.initcal(
        ms_hanning, conf["ants"], conf["spw"], *gaintables, rnd=0, overwrite=False
    )

    # flag primary calibrator
    flagging.autoroutine(
        ms_hanning,
        fluxcal,
        "fluxcal",
        rnd=0,
        devscale=10,
        cutoff=4,
        datacolumn="residual",
        overwrite=False,
    )

    # plot data before and after flagging
    plot.flagging_before_after(ms_hanning, fluxcal, "fluxcal_round_0", overwrite=False)

    # perform initial calibration round 1
    gaintables = calibration.initcal(
        ms_hanning,
        name,
        conf["refant"][idx],
        conf["cal chans"][idx],
        priortables,
        rnd=1,
        overwrite=False,
    )

    # plot initial calibration
    plot.initcal(
        ms_hanning, conf["ants"], conf["spw"], *gaintables, rnd=1, overwrite=False
    )

    # flag primary calibrator
    flagging.autoroutine(
        ms_hanning,
        fluxcal,
        "fluxcal",
        rnd=1,
        devscale=10,
        cutoff=4.0,
        datacolumn="residual",
        overwrite=False,
    )

    # plot data before and after flagging
    plot.flagging_before_after(
        ms_hanning, calibrators, "fluxcal_round_1", overwrite=False
    )

    # flag secondary calibrators
    flagging.autoroutine(
        ms_hanning,
        phasecal,
        "phasecal",
        rnd=0,
        devscale=10,
        cutoff=4.0,
        datacolumn="corrected",
        overwrite=False,
    )

    # plot data before and after flagging
    plot.flagging_before_after(
        ms_hanning, calibrators, "phasecal_round_0", overwrite=False
    )

    # flux bootstrapping
    _, fluxbootgains = calibration.fluxboot(
        ms_hanning,
        name,
        conf["cal chans"][idx],
        conf["refant"][idx],
        conf["solint max"][idx],
        overwrite=False,
    )

    ms_calibrators = os.path.join(root, "calibrators.ms")

    # plot flux bootstrapping gains
    plot.fluxboot_gains(ms_calibrators, conf["ants"], *fluxbootgains, overwrite=True)

    # plot calibrator models
    plot.calibrator_models(ms_calibrators, overwrite=True)

    # flag secondary calibrators residual
    flagging.autoroutine(
        ms_hanning,
        phasecal,
        "phasecal",
        rnd=1,
        devscale=10,
        cutoff=4.0,
        datacolumn="residual",
        overwrite=False,
    )

    # plot data before and after flagging
    plot.flagging_before_after(
        ms_hanning, calibrators, "phasecal_round_1", overwrite=False
    )

    # final calibration
    gaintables = calibration.initcal(
        ms_hanning,
        name,
        conf["refant"][idx],
        conf["cal chans"][idx],
        priortables,
        rnd=2,
        overwrite=False,
    )

    plot.initcal(
        ms_hanning, conf["ants"], conf["spw"], *gaintables, rnd=2, overwrite=False
    )

    gaintables = gaintables[1:]

    # final amplitude and phase calibration
    gaintables = calibration.finalcal(
        ms_hanning,
        name,
        conf["refant"][idx],
        conf["cal chans"][idx],
        conf["solint max"][idx],
        gaintables,
    )

    # apply calibration
    calibration.apply(ms_hanning, gaintables)

    # flag targets with devscale=5, cutoff=4, round 0
    flagging.autoroutine(
        ms_hanning,
        targets,
        "targets",
        rnd=0,
        devscale=5,
        cutoff=4,
        datacolumn="corrected",
    )
    plot.flagging_before_after(ms_hanning, targets, "targets_round_0")

    # make summary plots
    plot.summary(ms_hanning, overwrite=False)

    # compute modified z-score
    inspect.get_mod_z_score_data(
        ms_hanning, masked=True, data_column="CORRECTED", overwrite=True
    )

    # prepare for imaging (split targets from measurement set and downweight outliers)
    imaging.prep(ms_hanning, overwrite=False)


if __name__ == "__main__":
    for obs in conf["obs list"][:1]:
        pipeline_1(obs)
