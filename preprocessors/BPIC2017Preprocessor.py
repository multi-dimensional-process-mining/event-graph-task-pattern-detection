from preprocessors import GeneralPreprocessor
import pandas as pd


class BPIC2017Preprocessor(GeneralPreprocessor.GeneralPreprocessor):

    def __init__(self, name_data_set, filename, column_names, separator, timestamp_format,
                 path_to_neo4j_import_directory, use_sample, sample_cases):
        super().__init__(name_data_set, filename, column_names, separator, timestamp_format,
                         path_to_neo4j_import_directory, use_sample, sample_cases)

    def preprocess(self):
        self.csv_data_set = pd.read_csv(f'raw_data/{self.filename}.csv', keep_default_na=True,
                                        usecols=self.column_names, sep=self.separator)
        self.csv_data_set.drop_duplicates(keep='first', inplace=True)
        self.csv_data_set.reset_index(drop=True, inplace=True)

        if self.use_sample:
            self.csv_data_set = self.csv_data_set[self.csv_data_set['case'].isin(self.sample_cases)]

        self.csv_data_set.rename(columns={self.column_names[0]: "case", self.column_names[1]: "activity",
                                          self.column_names[2]: "timestamp", self.column_names[3]: "resource",
                                          self.column_names[4]: "lifecycle"}, inplace=True)

        self.csv_data_set['timestamp'] = pd.to_datetime(self.csv_data_set['timestamp'], format=self.timestamp_format)
        self.csv_data_set.fillna(0)
        self.csv_data_set.sort_values(['case', 'timestamp'], inplace=True)
        self.csv_data_set['timestamp'] = self.csv_data_set['timestamp'].map(
            lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + '+0100')

        self.csv_data_set.to_csv(f'{self.path_to_neo4j_import_directory}{self.name_data_set}.csv', index=True,
                                 index_label="idx")
