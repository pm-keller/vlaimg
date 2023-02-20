import os
import sys
import yaml
import casatools
import numpy as np
from prefect import flow

sys.path.append(os.getcwd())
from vlapy import vladata, plot, flagging, calibration, inspect
from vlapy.hanning import hanning

import matplotlib

matplotlib.use("agg")

# read yaml configuration file
with open("input/config.yaml", "r") as file:
    conf = yaml.safe_load(file)


@flow
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

    # make data directories
    vladata.makedir(conf["root"], obs)

    # hanning smoothing
    ms_hanning = hanning(ms)

    # make observation plots
    obsfiles = plot.plotobs(ms_hanning)

    # save some metadata to file
    ntimes = vladata.get_ntimes(ms_hanning)

    # VLA deterministic flags
    detflags_summary = flagging.detflags(
        ms_hanning,
        conf["flagging"]["quack"],
        conf["flagging"]["spw edge chan"],
        conf["spw"],
        conf["flagging"]["apriori"],
    )

    # VLA prior calibration
    priortables = calibration.priorcal(ms_hanning, obs.split(".")[0])

    # compute and plot primary calibrator model
    setjy_summary = calibration.setjy(ms_hanning)
    setjy_plot = plot.setjy_model_amp_vs_uvdist(ms_hanning)

    # compute modified z-score
    zfile = inspect.get_mod_z_score_data(ms, masked=True, ntimes=ntimes)

    # plot amplitude vs. frequency to find dead antennas
    dead_ant_plots = plot.find_dead_ants_amp_vs_freq(ms_hanning)


@flow
def pipeline_init_cal(obs):
    """
    The second part of the VLA data processing pipeline.
    This can only be performed once the outpurs of the first
    part of the pipeline have been visually inspected
    This part of the pipeline performs the initial calibration
    and might need to be repeated after inspection of the
    calibration tables.
    """

    print(f"\initial calibration on observation {obs}")

    # path to measurement set
    ms = os.path.join(conf["root"], obs, obs + "_hanning.ms")
    root = os.dirname(ms)
    name = obs.split(".")[0]

    # get flux calibratior
    msmd = casatools.msmetadata()
    msmd.open(ms)
    field = msmd.fieldsforintent("CALIBRATE_FLUX#UNSPECIFIED")[0]

    # get index of observation
    idx = np.where(conf["obs list"] == obs)[0]

    # prior calibration tables
    opacity_table = os.path.join(root, f"caltables/{name}.opac")
    rq_table = os.path.join(root, f"caltables/{name}.rq")
    swpow_table = os.path.join(root, f"caltables/{name}.swpow")
    antpos_table = antpos_table = os.path.join(root, f"caltables/{name}.antpos")
    priortables = [opacity_table, rq_table, swpow_table, antpos_table]

    # perform initial calibration
    gaintables = calibration.initcal(
        ms,
        name,
        field,
        conf["refant"][idx],
        conf["cal chans"][idx],
        priortables,
        round=0,
    )

    # plot calibration tables and corrected data
    initplots = plot.initcal(ms, field, *gaintables)


if __name__ == "__main__":
    for obs in conf["obs list"][:1]:
        # pipeline_1(obs)
        pipeline_init_cal(obs)
