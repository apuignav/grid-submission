#!/usr/bin/env python2

import os
import tempfile

from dirac import split_input_data, DiracException


def split_lfns(lfns, script_name, max_files_per_job=10):
    """Split LFNs and give the Gaudi options to use them.

    This function uses `splitInputData` from the DIRAC API.

    Arguments:
        lfns (list): LFNs to split.
        script_name (str): Name of the Gaudi options to use
            the LFNs.
        max_files_per_job (int, optional): Maximum files per job.
            Defaults to 10.

    Returns:
        list of tuple: LFNs to be used as input data and path of
            the script needed to use them.

    Raises:
        DiracException: When the DIRAC call fails.

    """
    base_script = """from Gaudi.Configuration import *
from GaudiConf import IOHelper
from Gaudi.Configuration import FileCatalog
IOHelper().inputFiles([{}], clear=True)
FileCatalog().Catalogs = ['xmlcatalog_file:"pool_xml_catalog.xml"']
"""
    split_groups = split_input_data(lfns)
    output = []
    for lfns in split_groups:
        script = base_script.format('\n'.join("'LFN:%s'," % lfn for lfn in lfns))
        temp_dir = tempfile.mkdtemp()
        script_path = os.path.join(temp_dir, script_name)
        with open(script_path, 'w') as script_file:
            script_file.write(script)
        output.append((lfns, script_path))
    return output

# EOF

