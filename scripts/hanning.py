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
import shutil
import casatasks

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--msin", help="Path to input measurement set.", type=str)
parser.add_argument("--msout", help="Path to output measurement set.", type=str)
args = parser.parse_args()

# hanning smoothing
if os.path.exists(args.msout):
    shutil.rmtree(args.msout)

casatasks.hanningsmooth(args.msin, outputvis=args.msout)