from preprocessors.BPIC2017Preprocessor import BPIC2017Preprocessor
from preprocessors.BPIC2014Preprocessor import BPIC2014Preprocessor
from preprocessors.GeneralPreprocessor import GeneralPreprocessor


def get_preprocessor(dataset, filename, column_names, separator, timestamp_format, path_to_neo4j_import_directory,
                     implementation, use_sample=False, sample_cases=[]):
    if dataset in ["bpic2017_single_ek", "bpic2017_single_ek_sample",
                   "bpic2017_multi_SCSR", "bpic2017_multi_SCMR", "bpic2017_multi_MCSR", "bpic2017_multi_MCMR",
                   "bpic2017_multi_SCSR_sample", "bpic2017_multi_SCMR_sample",
                   "bpic2017_multi_MCSR_sample", "bpic2017_multi_MCMR_sample"]:
        return BPIC2017Preprocessor(name_data_set=dataset, filename=filename, column_names=column_names,
                                    separator=separator, timestamp_format=timestamp_format,
                                    path_to_neo4j_import_directory=path_to_neo4j_import_directory,
                                    implementation=implementation, use_sample=use_sample, sample_cases=sample_cases)
    elif dataset == "bpic2014_single_ek":
        return BPIC2014Preprocessor(name_data_set=dataset, filename=filename, column_names=column_names,
                                    separator=separator, timestamp_format=timestamp_format,
                                    path_to_neo4j_import_directory=path_to_neo4j_import_directory,
                                    implementation=implementation, use_sample=use_sample, sample_cases=sample_cases)

    else:
        return GeneralPreprocessor(name_data_set=dataset, filename=filename, column_names=column_names,
                                   separator=separator, timestamp_format=timestamp_format,
                                   path_to_neo4j_import_directory=path_to_neo4j_import_directory,
                                   implementation=implementation, use_sample=use_sample, sample_cases=sample_cases)
