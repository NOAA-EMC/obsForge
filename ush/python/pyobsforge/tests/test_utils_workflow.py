import os
import shutil
import tempfile

import pytest

from pyobsforge.utils.workflow import gen_rocoto_xml
from pyobsforge import pyobsforge_directory

#     gen_rocoto_xml(user_inputs.config, user_inputs.template, user_inputs.output)
def test_gen_rocoto_xml():
    """
    Test the generation of a Rocoto XML from a config and a template
    """
    # define paths and create temp directory
    HOMEobsforge = os.path.abspath(os.path.join(pyobsforge_directory, '../../../'))
    config_path = os.path.join(HOMEobsforge, 'parm', 'config.yaml')
    template_path = os.path.join(HOMEobsforge, 'parm', 'obsforge_rocoto_template.xml.j2')
    base_dir = tempfile.mkdtemp()
    output_path = os.path.join(base_dir, 'test.xml')
    # run the function
    gen_rocoto_xml(config_path, template_path, output_path)
    assert os.path.exists(output_path)
    # TODO perhaps later when things stabilize have it read in a file to confirm
    # substitutions are made properly but no sense doing that at this time...
    shutil.rmtree(base_dir)
