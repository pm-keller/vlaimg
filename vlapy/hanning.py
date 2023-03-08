#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""

hanning.py

Created on: 2023/02/03
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: Apply hanning smoothing to data. 
This mitigates the Gibbs phenomenon around strong RFI spikes
and reduces the spectral resolution and thereby the data volume
by a factor of two. 

"""

import os
import time
import shutil
import casatasks

from prefect import task
from prefect.tasks import task_input_hash


# @task(cache_key_fn=task_input_hash)
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
        shutil.rmtree(output_ms)

    if os.path.exists(output_ms + ".flagversions") and overwrite:
        print(f"\nRemoving {output_ms + '.flagversions'} \n")
        shutil.rmtree(output_ms + ".flagversions")

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
