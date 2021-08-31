from labelmakers import GeneralLabelMaker


class BPIC2014LabelMaker(GeneralLabelMaker.LabelMaker):

    def __init__(self, use_label_dict, activity_label, print_duration, activity_label_sep):
        super().__init__(activity_label, print_duration, activity_label_sep)
        self.activity_label_sep = " "
        self.use_label_dict = use_label_dict
        if use_label_dict:
            self.node_label_font_size = "30"
        self.c_id_start = 4
        self.r_id_start = 4

    def get_event_label(self, record, element):
        if self.use_label_dict:
            print("No abbreviated labels available.")
            # node_label = abbr_dictionary.get(record[element]["activity_lifecycle"])
        else:
            activity_name = record[element][self.activity_label_combined]
            space_positions = [pos for pos, char in enumerate(activity_name) if char == self.activity_label_sep]
            # if not space_positions:
            #     node_label = activity_name[0:5]
            #     # print(node_label)
            # elif len(space_positions) == 1:
            #     node_label = activity_name[0:min(5, space_positions[0])] + "\n" \
            #                  + activity_name[space_positions[0] + 1:space_positions[0] + 5]
            #     # print(node_label)
            # elif len(space_positions) > 1:
            #     node_label = activity_name[0:min(5, space_positions[0])] + "\n" \
            #                  + activity_name[space_positions[0] + 1:min(space_positions[0] + 6, space_positions[1])] \
            #                  + "\n" + activity_name[space_positions[1] + 1:space_positions[1] + 5]
            #     # print(node_label)
            if not space_positions:
                node_label = activity_name[0:5]
                # print(node_label)
            else:
                node_label = activity_name[0:min(5, space_positions[0])] + "\n" \
                             + activity_name[space_positions[0] + 1:space_positions[0] + 5]
        return node_label

    def get_case_label(self, case_id):
        case_label = str(case_id)[self.c_id_start:]
        return case_label

    def get_resource_label(self, resource_id):
        resource_label = str(resource_id)[self.r_id_start:]
        return resource_label
