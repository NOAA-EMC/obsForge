#!/usr/bin/env python3

from wxflow import AttrDict, Jinja, YAMLFile


def gen_rocoto_xml(config_path, template_path, output_path):
    """
    gen_rocoto_xml:

    Generate a Rocoto XML file from an input
    YAML configuration and Jinja XML template.

    Args:
    - config_path: string path to YAML config
    - template_path: string path to Jinja2 XML template
    - output_path: string path of where to write output XML
    """
    # open input YAML configuration
    config_input = YAMLFile(path=config_path)
    # loop over keys and create a new config dict
    config = AttrDict()
    for key, value in config_input.items():
        config.update(value)
    # open Jinja XML template
    xml_jinja = Jinja(template_path, config, allow_missing=True)
    # save XML file
    xml_jinja.save(output_path)
