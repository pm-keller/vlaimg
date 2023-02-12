#!/lustre/aoc/projects/hera/pkeller/anaconda3/envs/vlaimg3.8/bin/python3
# fmt: off
#SBATCH -p hera
#SBATCH -J img
#SBATCH -t 12:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=5
#SBATCH --mem=128G
#SBATCH --mail-type=ALL
#SBATCH --mail-user pmk46@cam.ac.uk
# -*-coding:utf-8 -*-

"""

imaging.py

Created on: 2023/01/27
Author: Pascal M. Keller 
Affiliation: Cavendish Astrophysics, University of Cambridge, UK
Email: pmk46@cam.ac.uk

Description: CASA Imaging Pipeline

"""

import os
import glob
import yaml
import shutil
import casatasks

from multiprocessing import Pool

# fmt: on
# read yaml file
with open("obs.yaml", "r") as file:
    obs = yaml.safe_load(file)


for spw in ["10~15"]:
    print(f"\n{spw}")

    def make_image(field):
        # multi-frequency synthesis, clean imaging
        target_vis = os.path.join(obs["root"], field + ".ms")
        target_im = os.path.join(obs["root"], field + f"_L-Band_mtmfs_spw_{spw}")

        if os.path.exists(target_im):
            shutil.rmtree(glob.glob(target_im + "*"))
        """
        # dirty image
        casatasks.tclean(
            target_vis,
            imagename=target_im + "_dirty",
            spw=spw,
            cell="0.4arcsec",
            imsize=[8000, 8000],
            niter=0,
            pblimit=-0.1,
            specmode="mfs",
            deconvolver="mtmfs",
            nterms=2,
            weighting="natural",
            stokes="I",
            savemodel="modelcolumn",
            pbcor=False,
            interactive=False,
        )
        """
        # clean image
        casatasks.tclean(
            target_vis,
            imagename=target_im,
            spw=spw,
            cell="0.4arcsec",
            imsize=[4000, 4000],
            niter=10000,
            gain=0.1,
            pblimit=-0.1,
            pbmask=0.5,
            nsigma=5,
            specmode="mfs",
            deconvolver="mtmfs",
            nterms=2,
            weighting="natural",
            stokes="I",
            savemodel="modelcolumn",
            pbcor=False,
            interactive=False,
        )

    with Pool(processes=5) as pool:
        pool.map(make_image, obs["fields"]["all"])
