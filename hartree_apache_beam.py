# it appears our file is not of conventional csv format

import csv

input_file1 = "./data/hartree/dataset1.csv"
output_file1 = "./data/hartree/corrected_dataset1.csv"

# Read the input file
with open(input_file1, "r") as file:
    lines = file.readlines()

# Write the lines to a CSV file
with open(output_file1, "w", newline="") as file:
    writer = csv.writer(file)
    for line in lines:
        writer.writerow(line.strip().split(","))

# correct seconds csv as well

input_file2 = "./data/hartree/dataset2.csv"
output_file2 = "./data/hartree/corrected_dataset2.csv"

# Read the input file
with open(input_file2, "r") as file:
    lines = file.readlines()

# Write the lines to a CSV file
with open(output_file2, "w", newline="") as file:
    writer = csv.writer(file)
    for line in lines:
        writer.writerow(line.strip().split(","))


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self, source_pipeline_name, source_data, join_pipeline_name, join_data, common_key):
        self.join_pipeline_name = join_pipeline_name
        self.source_data = source_data
        self.source_pipeline_name = source_pipeline_name
        self.join_data = join_data
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

        """This part here below starts with a python dictionary comprehension in case you
        get lost in what is happening :-)"""
        return ({pipeline_name: pcoll | 'Convert to ({0}, object) for {1}'
                .format(self.common_key, pipeline_name)
                                >> beam.Map(_format_as_common_key_tuple, self.common_key)
                 for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(),
                                                   self.source_pipeline_name,
                                                   self.join_pipeline_name)
                )

class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


class LogContents(beam.DoFn):
    """This DoFn class logs the content of that which it receives """

    def process(self, input_element):
        logging.info("Contents: {}".format(input_element))
        logging.info("Contents type: {}".format(type(input_element)))
        logging.info("Contents Access input_element['Country']: {}".format(input_element['Country']))
        return


def map_to_dict(element, header):
    # Split the element by comma to get individual values
    values = element.split(',')

    # Create a dictionary by zipping the header columns with the corresponding values
    return dict(zip(header, values))

def convert_to_csv(element):
    csv_row = ','.join([str(value) for value in element.values()])
    return csv_row


def run(argv=None):
    """Main entry point"""
    dataset1_path = './data/hartree/corrected_dataset1.csv'
    dataset2_path = './data/hartree/corrected_dataset2.csv'

    with open(dataset1_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        headers_1 = next(reader)

    with open(dataset2_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        headers_2 = next(reader)

    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as pipeline:
        dataset1 = (
                pipeline
                | 'read from dataset1' >> beam.io.ReadFromText(dataset1_path, skip_header_lines=1)
                | 'format dataset1 with header in dict' >> beam.Map(lambda row: map_to_dict(row, headers_1))
        )

        dataset2 = (
                pipeline
                | 'read from dataset2' >> beam.io.ReadFromText(dataset2_path, skip_header_lines=1)
                | 'format dataset2 with header in dict' >> beam.Map(lambda row: map_to_dict(row, headers_2))
        )

        common_key = 'counter_party'
        source_pipeline_name = 'dataset1'
        join_pipeline_name = 'dataset2'
        pipelines_dictionary = {source_pipeline_name: dataset1,
                                join_pipeline_name: dataset2}
        joined_data = (
                pipelines_dictionary
                | 'Left join' >> LeftJoin(source_pipeline_name, dataset1, join_pipeline_name, dataset2, common_key)
                | 'Convert values col to int' >> beam.Map(
            lambda element: {'invoice_id': element[headers_1[0]], 'legal_entity': element[headers_1[1]],
                             'counter_party': element[headers_1[2]], 'rating': element[headers_1[3]],
                             'status': element[headers_1[4]], 'value': int(element[headers_1[5]]),
                             'tier': element[headers_2[1]]})
        )

        max_rating_by_party_ = 'max_rating_by_party'
        max_rating_by_party = (
                joined_data
                | 'Map Key-Value for rating and party' >> beam.Map(
            lambda element: (element[headers_1[2]], element[headers_1[3]]))
                | 'group by counter_party and max rating' >> beam.CombinePerKey(max)
                | 'reformat max_rating_by_party to dict' >> beam.Map(
            lambda element: {headers_1[2]: element[0], max_rating_by_party_: element[1]})
        )

        total_legal_entity_ = 'max_legal_entity'
        total_legal_entity = (
                joined_data
                | 'Map Key-Value for tiers and value' >> beam.Map(
            lambda element: (element[headers_1[1]], element[headers_1[5]]))
                | 'Group by tiers and sum values' >> beam.CombinePerKey(sum)
                | 'reformat total_legal_entity to dict' >> beam.Map(
            lambda element: {headers_1[1]: element[0], total_legal_entity_: element[1]})
        )

        total_counter_party_ = 'total_counter_party'
        total_counter_party = (
                joined_data
                | 'Map Key-Value for counter_party and value' >> beam.Map(
            lambda element: (element[headers_1[2]], element[headers_1[5]]))
                | 'Group by counter_party sum values' >> beam.CombinePerKey(sum)
                | 'reformat total_counter_party to dict' >> beam.Map(
            lambda element: {headers_1[2]: element[0], total_counter_party_: element[1]})
        )

        total_status_ = 'total_status'
        total_status = (
                joined_data
                | 'Map Key-Value for status and value' >> beam.Map(
            lambda element: (element[headers_1[4]], element[headers_1[5]]))
                | 'Group by status sum values' >> beam.CombinePerKey(sum)
                | 'reformat total_status to dict' >> beam.Map(
            lambda element: {headers_1[4]: element[0], total_status_: element[1]})
        )

        total_tier_ = 'total_tier'
        total_tier = (
                joined_data
                | 'Map Key-Value for tier and value' >> beam.Map(
            lambda element: (element[headers_2[1]], element[headers_1[5]]))
                | 'Group by tier sum values' >> beam.CombinePerKey(sum)
                | 'reformat total_tier to dict' >> beam.Map(
            lambda element: {headers_2[1]: element[0], total_tier_: element[1]})
        )

        joined_data_ = 'joined_data'
        joined_data = (
                {joined_data_: joined_data, max_rating_by_party_: max_rating_by_party}
                | 'Left join {}'.format(headers_1[3]) >> LeftJoin(joined_data_, joined_data, max_rating_by_party_,
                                                                  max_rating_by_party, headers_1[2])
        )

        joined_data = (
                {joined_data_: joined_data, total_legal_entity_: total_legal_entity}
                | 'Left join {}'.format(headers_1[1]) >> LeftJoin(joined_data_, joined_data, total_legal_entity_,
                                                                  total_legal_entity, headers_1[1])
        )

        joined_data = (
                {joined_data_: joined_data, total_counter_party_: total_counter_party}
                | 'Left join {}'.format(headers_1[2]) >> LeftJoin(joined_data_, joined_data, total_counter_party_,
                                                                  total_counter_party, headers_1[2])
        )

        joined_data = (
                {joined_data_: joined_data, total_status_: total_status}
                | 'Left join {}'.format(headers_1[4]) >> LeftJoin(joined_data_, joined_data, total_status_,
                                                                  total_status, headers_1[4])
        )

        joined_data = (
                {joined_data_: joined_data, total_tier_: total_tier}
                | 'Left join {}'.format(headers_2[1]) >> LeftJoin(joined_data_, joined_data, total_tier_, total_tier,
                                                                  headers_2[1])
        )

        joined_data = (
                joined_data
                | 'write csv to file' >> beam.io.WriteToText('./data/hartree/result.csv')
        )

if __name__ == '__main__':
    run()
