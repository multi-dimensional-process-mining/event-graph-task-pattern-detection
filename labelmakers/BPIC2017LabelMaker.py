from labelmakers import GeneralLabelMaker
from vis_reference.bpic2017_dictionaries import abbr_dictionary


class BPIC2017LabelMaker(GeneralLabelMaker.GeneralLabelMaker):

    def __init__(self, use_label_dict, activity_label, print_duration, activity_label_sep):
        super().__init__(activity_label, print_duration, activity_label_sep)
        self.use_label_dict = use_label_dict
        if use_label_dict:
            self.node_label_font_size = "30"
        self.c_id_start = 12
        self.r_id_start = 5

    def get_event_label(self, record, element):
        if self.use_label_dict:
            node_label = abbr_dictionary.get(record[element][self.activity_label_combined])
        else:
            node_label = record[element][self.activity_label[0]][0:1] + '\n' + record[element][self.activity_label[0]][2:8] \
                         + '\n' + record[element][self.activity_label[1]][0:4]
        return node_label

    def get_case_label(self, case_id):
        case_label = str(case_id)[self.c_id_start:]
        return case_label

    def get_resource_label(self, resource_id):
        resource_label = str(resource_id)[self.r_id_start:]
        return resource_label
