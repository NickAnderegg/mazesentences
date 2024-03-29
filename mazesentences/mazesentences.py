#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# The MIT License (MIT)
#
# Copyright (c) 2016 Your Name
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

#add correct version number here
__version__ = "0.1.0"


PROGRAMNAME="mazesentences"
VERSION=__version__
COPYRIGHT="(C) 2016 Nick Anderegg"

from .stimulusprocessor import *
import json

def main():
    # reprocess_trials()
    # get_sentences()
    # _get_incomplete_sets()
    # generate_sample(76, rand=False)
    # generate_sample(8, rand=False, choices=[34,26,75,56,18,25,38])
    # generate_sentences_raw()
    # recombine_sentences(25)
    regenerate_distractors()

if __name__ == "__main__":
    main()
