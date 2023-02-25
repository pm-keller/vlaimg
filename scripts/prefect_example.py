import os
import glob
import casatasks
import casaplotms
import time
import shutil

from prefect import task, flow
from prefect.tasks import task_input_hash

# set display environment variable
from pyvirtualdisplay import Display

display = Display(visible=0, size=(2048, 2048))
display.start()


@task(cache_key_fn=task_input_hash)
def hanning(input_ms, overwrite=False):
    """hanning smooth measurement set

    Parameters
    ----------
    input_ms : str
        path to input measurement set
    overwrite : bool, optional
        if true, overwrite existing data, by default False

    Returns
    -------
    str
        path to output measurement set
    """

    output_ms = input_ms[:-3] + "_hanning.ms"

    # remove existing file?
    if os.path.exists(output_ms) and overwrite:
        print(f"\nRemoving {output_ms} \n")
        time.sleep(3)
        shutil.rmtree(output_ms)

    # hanning smoothing
    if not os.path.exists(output_ms):
        print("\nhanning smooth")
        print(f"input ms: {input_ms}")
        print(f"output ms: {output_ms}")

        casatasks.hanningsmooth(input_ms, output_ms)

        # save original flags
        print(f"\nsave flag version: original")
        casatasks.flagmanager(output_ms, mode="save", versionname=f"original")
    else:
        print(f"\nrestoring flag version: original")
        casatasks.flagmanager(output_ms, mode="restore", versionname=f"original")

    return output_ms


@task(cache_key_fn=task_input_hash)
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


@flow
def flow(ms):
    hanning(ms)
    plotobs(ms)


if __name__ == "__main__":
    ms = "/DATA/CARINA_3/kel334/19A-056/19A-056.sb37262953.eb37267948.58744.511782789355/19A-056.sb37262953.eb37267948.58744.511782789355.ms"
    flow(ms)
