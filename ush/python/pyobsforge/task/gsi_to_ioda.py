#!/usr/bin/env python3

import os
import glob
import gsincdiag_to_ioda.proc_gsi_ncdiag as gsid
import gsincdiag_to_ioda.combine_obsspace as gsid_combine
import gzip
import tarfile
from logging import getLogger
from pprint import pformat
from typing import Optional, Dict, Any

from wxflow import (AttrDict,
                    FileHandler,
                    add_to_datetime, to_timedelta,
                    Task,
                    parse_j2yaml,
                    logit)

logger = getLogger(__name__.split('.')[-1])

class GsiToIoda(Task):
    """
    Class for converting GSI diag files and bias correction files
    for use by JEDI
    - IODA files for observations
    - UFO readable netCDF files for bias correction
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                'window_begin': _window_begin,
                'window_end': _window_end,
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                # 'COMIN_OBSPROC': os.path.join(self.task_config.OBSPROC_COMROOT,
                #                               f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                #                               f"{self.task_config.cyc:02d}",
                #                               'atmos'),
                'COMIN_ATMOS_ANALYSIS': os.path.join(self.task_config.COMROOT_PROD, "gfs", "v16.3",
                                                     f"gdas.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                                     f"{self.task_config.cyc:02d}", "atmos"),
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)

    @logit(logger)
    def convert_gsi_diags(self) -> None:
        """Convert GSI diag files to ioda-stat files for analysis stats

        This method will convert GSI diag files to ioda-stat files for analysis stats.
        This includes:
        - copying GSI diag files to DATA path
        - untarring and gunzipping GSI diag files
        - converting GSI diag files to ioda files using gsincdiag2ioda converter scripts

        Parameters
        ----------
        None

        Returns
        ----------
        None
        """
        logger.info("Converting GSI diag files to IODA files for analysis stats")
        # copy GSI diag files to DATA path
        diag_tars = ['cnvstat', 'radstat', 'oznstat']
        diag_dir_ges_path = os.path.join(self.task_config.DATA, 'atmos_gsi_ges')
        diag_dir_anl_path = os.path.join(self.task_config.DATA, 'atmos_gsi_anl')
        diag_dir_path = os.path.join(self.task_config.DATA, 'atmos_gsi_diags')
        FileHandler({'mkdir': [diag_dir_path, diag_dir_ges_path, diag_dir_anl_path]}).sync()
        diag_ioda_dir_ges_path = os.path.join(self.task_config.DATA, 'atmos_gsi_ioda_ges')
        diag_ioda_dir_anl_path = os.path.join(self.task_config.DATA, 'atmos_gsi_ioda_anl')
        output_dir_path = os.path.join(self.task_config.DATA, 'atmos_gsi_ioda')
        FileHandler({'mkdir': [diag_ioda_dir_ges_path, diag_ioda_dir_anl_path, output_dir_path]}).sync()
        diag_tar_copy_list = []
        for diag in diag_tars:
            input_tar_basename = f"{self.task_config.APREFIX}{diag}"
            input_tar = os.path.join(self.task_config.COMIN_ATMOS_ANALYSIS,
                                     input_tar_basename)
            dest = os.path.join(diag_dir_path, input_tar_basename)
            if os.path.exists(input_tar):
                diag_tar_copy_list.append([input_tar, dest])
        FileHandler({'copy_opt': diag_tar_copy_list}).sync()

        # Untar and gunzip diag files
        gsi_diag_tars = glob.glob(os.path.join(diag_dir_path, f"{self.task_config.APREFIX}*stat"))
        for diag_tar in gsi_diag_tars:
            logger.info(f"Untarring {diag_tar}")
            with tarfile.open(diag_tar, "r") as tar:
                tar.extractall(path=diag_dir_path)
        gsi_diags = glob.glob(os.path.join(diag_dir_path, "diag_*.nc4.gz"))
        for diag in gsi_diags:
            logger.info(f"Gunzipping {diag}")
            output_file = diag.rstrip('.gz')
            with gzip.open(diag, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    f_out.write(f_in.read())
            os.remove(diag)

        # Copy diag files to ges or anl directory
        anl_diags = glob.glob(os.path.join(diag_dir_path, "diag_*_anl*.nc4"))
        ges_diags = glob.glob(os.path.join(diag_dir_path, "diag_*_ges*.nc4"))
        copy_anl_diags = []
        for diag in anl_diags:
            copy_anl_diags.append([diag, os.path.join(diag_dir_anl_path, os.path.basename(diag))])
        FileHandler({'copy_opt': copy_anl_diags}).sync()
        copy_ges_diags = []
        for diag in ges_diags:
            copy_ges_diags.append([diag, os.path.join(diag_dir_ges_path, os.path.basename(diag))])
        FileHandler({'copy_opt': copy_ges_diags}).sync()

        # Convert GSI diag files to ioda files using gsincdiag2ioda converter scripts
        gsid.proc_gsi_ncdiag(ObsDir=diag_ioda_dir_ges_path, DiagDir=diag_dir_ges_path)
        gsid.proc_gsi_ncdiag(ObsDir=diag_ioda_dir_anl_path, DiagDir=diag_dir_anl_path)

        # now we need to combine the two sets of ioda files into one file
        # by adding certain groups from the anl file to the ges file
        ges_ioda_files = glob.glob(os.path.join(diag_ioda_dir_ges_path, '*nc'))
        for ges_ioda_file in ges_ioda_files:
            anl_ioda_file = ges_ioda_file.replace('_ges_', '_anl_').replace(diag_ioda_dir_ges_path, diag_ioda_dir_anl_path)
            if os.path.exists(anl_ioda_file):
                logger.info(f"Combining {ges_ioda_file} and {anl_ioda_file}")
                out_ioda_file = os.path.join(output_dir_path, os.path.basename(ges_ioda_file).replace('_ges_', '_gsi_'))
                gsid.combine_ges_anl_ioda(ges_ioda_file, anl_ioda_file, out_ioda_file)
            else:
                logger.warning(f"WARNING: {anl_ioda_file} does not exist to combine with {ges_ioda_file}")
                logger.warning("Skipping this file ...")

        # now run combine obsspace on conventional files to get final ioda files
        conv_types = ['sondes','aircraft', 'sfc', 'sfcship']
        for conv_type in conv_types:
            conv_file_list = glob.glob(os.path.join(output_dir_path, f'*{conv_type}_*.nc'))
            conv_output_file_path = os.path.join(output_dir_path, f'{conv_type}_gsi_{self.task_config.current_cycle.strftime("%Y%m%d%H")}.nc')
            logger.info(f"Combining {len(conv_file_list)} {conv_type} files to {conv_output_file_path}")
            gsid_combine.combine_obsspace(conv_file_list, conv_output_file_path, False)
            # remove individual conv_type files
            for conv_file in conv_file_list:
                os.remove(conv_file)

        # Copy the output IODA files to COMOUT
        # define output COMOUT path
        comout = os.path.join(self.task_config['COMROOT'],
                        self.task_config['PSLOT'],
                        f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                        f"{self.task_config.cyc:02d}",
                        'atmos_gsi')
        if not os.path.exists(comout):
            FileHandler({'mkdir': [comout]}).sync()
        copy_ioda_files = []
        # get list of output ioda files to copy to COMOUT
        output_ioda_files = glob.glob(os.path.join(output_dir_path, '*nc'))
        for ioda_file in output_ioda_files:
            dest = os.path.join(comout, os.path.basename(ioda_file))
            copy_ioda_files.append([ioda_file, dest])
        logger.info(f"Copying {len(copy_ioda_files)} GSI IODA files to {comout}")
        FileHandler({'copy_opt': copy_ioda_files}).sync()
        

        # # Tar up the ioda files
        # iodastatzipfile = os.path.join(self.task_config.DATA, 'atmos_gsi_ioda',
        #                                f"{self.task_config.APREFIX}atmos_gsi_ioda_diags.tar.gz")
        # logger.info(f"Compressing GSI IODA files to {iodastatzipfile}")
        # # get list of iodastat files to put in tarball
        # iodastatfiles = glob.glob(os.path.join(output_dir_path, '*nc4'))
        # logger.info(f"Gathering {len(iodastatfiles)} GSI IODA files to {iodastatzipfile}")
        # with tarfile.open(iodastatzipfile, "w|gz") as archive:
        #     for targetfile in iodastatfiles:
        #         # gzip the file before adding to tar
        #         with open(targetfile, 'rb') as f_in:
        #             with gzip.open(f"{targetfile}.gz", 'wb') as f_out:
        #                 f_out.writelines(f_in)
        #         os.remove(targetfile)
        #         targetfile = f"{targetfile}.gz"
        #         archive.add(targetfile, arcname=os.path.basename(targetfile))
        # logger.info(f"Finished compressing GSI IODA files to {iodastatzipfile}")
        # # copy to COMOUT
        # outdir = self.task_config.COMOUT_ATMOS_ANALYSIS
        # if not os.path.exists(outdir):
        #     FileHandler({'mkdir': [outdir]}).sync()
        # dest = os.path.join(outdir, os.path.basename(iodastatzipfile))
        # logger.info(f"Copying {iodastatzipfile} to {dest}")
        # FileHandler({'copy_opt': [[iodastatzipfile, dest]]}).sync()
        # logger.info("Finished copying GSI IODA tar file to COMOUT")

    def convert_bias_correction_files(self) -> None:
        """Convert bias correction files to UFO readable netCDF files

        This method will convert bias correction files to UFO readable netCDF files.
        This includes:
        - copying bias correction files to DATA path
        - converting bias correction files to UFO readable netCDF files

        Parameters
        ----------
        None

        Returns
        ----------
        None
        """
        logger.info("Converting bias correction files to UFO readable netCDF files")
        