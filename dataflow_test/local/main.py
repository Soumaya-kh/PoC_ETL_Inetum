# This is an Apache Beam training Python script.

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import apache_beam as beam
from apache_beam.coders.coders import Coder

class ISOCoder(Coder):
    """A coder used for reading and writing strings as ISO-8859-1."""

    def encode(self, value):
        return value.encode('iso-8859-1')

    def decode(self, value):
        return value.decode('iso-8859-1')

    def is_deterministic(self):
        return True

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    with beam.Pipeline() as p:
        (p | beam.io.ReadFromText('raw_data_webngrams.csv', coder=ISOCoder(), skip_header_lines=1)
         | beam.Map(lambda x: x.split(';'))
         | beam.Filter(lambda ligne: ligne[3] == "en")
         #| beam.Map(print)
         | beam.io.WriteToText('filtered_data_webngrams', file_name_suffix='.csv')
         | beam.Map(print))


