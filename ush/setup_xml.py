#!/usr/bin/env python3

import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from logging import getLogger
from wxflow.jinja import Jinja


def input_args(*argv):
    """
    Method to collect user arguments for `setup_xml.py`
    """

    description = """
        Uses input config.yaml and XML Jinja2 template
        to create an output XML for use with Rocoto.
        """
    
    parser = ArgumentParser(description=description,
                            formatter_class=ArgumentDefaultsHelpFormatter)

    # grab arguments
    parser.add_argument('--config', help='path to input YAML configuration',
                        type=str, required=True)
    parser.add_argument('--template', help='path to XML Jinja2 template',
                        type=str, required=True)
    parser.add_argument('--output', help='path to output Rocoto XML file',
                        type=str, required=True)
    
    return parser.parse_args(argv[0][0] if len(argv[0]) else None)

def main(*argv):

    user_inputs = input_args(argv)

if __name__ == '__main__':

    main()
