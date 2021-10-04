import pandas as pd


class GeneralPreprocessor:

    def __init__(self, name_data_set, filename, column_names, separator, timestamp_format,
                 path_to_neo4j_import_directory, use_sample, sample_cases):
        self.name_data_set = name_data_set
        self.filename = filename
        self.column_names = column_names
        self.separator = separator
        self.timestamp_format = timestamp_format
        self.path_to_neo4j_import_directory = path_to_neo4j_import_directory
        self.csv_data_set = None
        self.use_sample = use_sample
        self.sample_cases = sample_cases

    def preprocess(self):

        self.csv_data_set = pd.read_csv(f'raw_data/{self.filename}.csv', keep_default_na=True,
                                        usecols=self.column_names, sep=self.separator)
        # self.csv_data_set.drop_duplicates(keep='first', inplace=True)

        if len(self.column_names) == 5:
            self.csv_data_set.rename(columns={self.column_names[0]: "case", self.column_names[1]: "activity",
                                              self.column_names[2]: "timestamp", self.column_names[3]: "resource",
                                              self.column_names[4]: "lifecycle"}, inplace=True)
        elif len(self.column_names) == 4:
            self.csv_data_set.rename(columns={self.column_names[0]: "case", self.column_names[1]: "activity",
                                              self.column_names[2]: "timestamp", self.column_names[3]: "resource"},
                                     inplace=True)
        else:
            print("Undesired amount of columns.")

        if self.use_sample:
            self.csv_data_set = self.csv_data_set[self.csv_data_set['case'].isin(self.sample_cases)]

        self.csv_data_set['timestamp'] = pd.to_datetime(self.csv_data_set['timestamp'], format=self.timestamp_format)
        self.csv_data_set['timestamp'] = self.csv_data_set['timestamp'].map(
            lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + '+0100')
        self.csv_data_set.reset_index(drop=True, inplace=True)
        # self.csv_data_set.fillna(0)
        # self.csv_data_set.sort_values(['case', 'timestamp'], inplace=True)
        # self.csv_data_set['timestamp'] = self.csv_data_set['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S')+'.000+0100')

        self.csv_data_set.to_csv(f'{self.path_to_neo4j_import_directory}{self.name_data_set}.csv', index=True,
                                 index_label="idx")
