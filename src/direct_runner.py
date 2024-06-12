# Using DirectRunner to test

import argparse
import logging
import re

import apache_beam as beam

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

input = '/home/andrub818/motor-vehicle-crashes/notebooks/data.json'
output = '/home/andrub818/motor-vehicle-crashes/data/output.json'
# example command:
# python -m apache_beam.examples.wordcount --input YOUR_INPUT_FILE --output counts


# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):

        def process(self, element):
            print(element)

    def expand(self, input):
        input | beam.ParDo(self._OutputFn())

def main(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default=input,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | 'Read' >> ReadFromText(known_args.input) \
            | beam.Filter(lambda line: line != "")

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        output = lines | 'Write' >> WriteToText(known_args.output)


        result = p.run()
        result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
