from preprocessors import GeneralPreprocessor
import pandas as pd


class BPIC2017Preprocessor(GeneralPreprocessor.Preprocessor):

    def __init__(self, name_data_set, filename, column_names, separator, timestamp_format,
                 path_to_neo4j_import_directory, implementation, use_sample):
        super().__init__(name_data_set, filename, column_names, separator, timestamp_format,
                         path_to_neo4j_import_directory, implementation, use_sample)

    def preprocess(self):
        self.csv_data_set = pd.read_csv(f'raw_data/{self.filename}.csv', keep_default_na=True,
                                        usecols=self.column_names, sep=self.separator)
        self.csv_data_set.drop_duplicates(keep='first', inplace=True)
        self.csv_data_set.reset_index(drop=True, inplace=True)

        # if self.use_sample:
        #     sample_cases = ['Application_2045572635',
        #                     'Application_2014483796',
        #                     'Application_1973871032',
        #                     'Application_1389621581',
        #                     'Application_1564472847',
        #                     'Application_430577010',
        #                     'Application_889180637',
        #                     'Application_1065734594',
        #                     'Application_681547497',
        #                     'Application_1020381296',
        #                     'Application_180427873',
        #                     'Application_2103964126',
        #                     'Application_55972649',
        #                     'Application_1076724533',
        #                     'Application_1639247005',
        #                     'Application_1465025013',
        #                     'Application_1244956957',
        #                     'Application_1974117177',
        #                     'Application_797323371',
        #                     'Application_1631297810']
        if self.use_sample:
            sample_cases = ['Application_1111458873',
                            'Application_1372864243',
                            'Application_206394826',
                            'Application_1877008365',
                            'Application_1992048266']
            self.csv_data_set = self.csv_data_set[self.csv_data_set['case'].isin(sample_cases)]

        if self.implementation[0] == "single":
            self.csv_data_set.rename(columns={self.column_names[0]: "case", self.column_names[1]: "activity",
                                              self.column_names[2]: "timestamp", self.column_names[3]: "resource",
                                              self.column_names[4]: "lifecycle"}, inplace=True)
        elif len(self.implementation[1]) == 1 and len(self.implementation[2]) == 1:
            self.csv_data_set.rename(columns={self.column_names[0]: "case", self.column_names[1]: "activity",
                                              self.column_names[2]: "timestamp", self.column_names[3]: "resource",
                                              self.column_names[4]: "lifecycle"}, inplace=True)
        elif len(self.implementation[1]) > 1 and len(self.implementation[2]) == 1:
            self.csv_data_set.rename(columns={self.column_names[0]: "case1", self.column_names[1]: "activity",
                                              self.column_names[2]: "timestamp", self.column_names[3]: "resource",
                                              self.column_names[4]: "lifecycle"}, inplace=True)
            self.csv_data_set['case2'] = self.csv_data_set['case1'].apply(lambda x: x + "b")
            self.csv_data_set['case3'] = self.csv_data_set['case1'].apply(lambda x: x + "c")
        elif len(self.implementation[1]) == 1 and len(self.implementation[2]) > 1:
            self.csv_data_set.rename(columns={self.column_names[0]: "case", self.column_names[1]: "activity",
                                              self.column_names[2]: "timestamp", self.column_names[3]: "resource1",
                                              self.column_names[4]: "lifecycle"}, inplace=True)
            self.csv_data_set['resource2'] = self.csv_data_set['resource1'].apply(lambda x: x + "b")
        else:
            self.csv_data_set.rename(columns={self.column_names[0]: "case1", self.column_names[1]: "activity",
                                              self.column_names[2]: "timestamp", self.column_names[3]: "resource1",
                                              self.column_names[4]: "lifecycle"}, inplace=True)
            self.csv_data_set['case2'] = self.csv_data_set['case1'].apply(lambda x: x + "b")
            self.csv_data_set['case3'] = self.csv_data_set['case1'].apply(lambda x: x + "c")
            self.csv_data_set['resource2'] = self.csv_data_set['resource1'].apply(lambda x: x + "b")

        self.csv_data_set['timestamp'] = pd.to_datetime(self.csv_data_set['timestamp'], format=self.timestamp_format)
        self.csv_data_set.fillna(0)
        self.csv_data_set.sort_values(['case', 'timestamp'], inplace=True)
        self.csv_data_set['timestamp'] = self.csv_data_set['timestamp'].map(
            lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] + '+0100')

        self.csv_data_set.to_csv(f'{self.path_to_neo4j_import_directory}{self.name_data_set}.csv', index=True,
                                 index_label="idx")
