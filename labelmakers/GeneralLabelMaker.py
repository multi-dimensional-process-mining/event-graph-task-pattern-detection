class GeneralLabelMaker:

    def __init__(self, activity_label, print_duration, activity_label_sep):
        self.print_duration = print_duration
        self.node_label_font_size = "13"
        self.activity_label = activity_label
        if len(activity_label) == 1:
            self.activity_label_combined = activity_label[0]
        else:
            self.activity_label_combined = activity_label[0] + "_" + activity_label[1]
        self.activity_label_sep = activity_label_sep

    def get_event_label(self, record, element):
        activity_name = record[element][self.activity_label_combined]
        space_positions = [pos for pos, char in enumerate(activity_name) if char == self.activity_label_sep]
        if not space_positions:
            node_label = activity_name[0:5]
            # print(node_label)
        elif len(space_positions) == 1:
            node_label = activity_name[0:min(5, space_positions[0])] + "\n" \
                         + activity_name[space_positions[0] + 1:space_positions[0] + 5]
            # print(node_label)
        elif len(space_positions) > 1:
            node_label = activity_name[0:min(5, space_positions[0])] + "\n" \
                         + activity_name[space_positions[0] + 1:min(space_positions[0] + 6, space_positions[1])] \
                         + "\n" + activity_name[space_positions[1] + 1:space_positions[1] + 5]
            # print(node_label)
        return node_label

    def get_node_label_font_size(self):
        return self.node_label_font_size

    def get_edge_label_duration(self, record):
        if self.print_duration:
            duration = record["duration"]
            str_duration = f"{(duration.hours_minutes_seconds[0] * 60) + duration.hours_minutes_seconds[1]}m" \
                           f"{duration.hours_minutes_seconds[2]:.0f}s"
            return str_duration
        else:
            return ""

    def get_case_label(self, case_id):
        case_label = str(case_id)
        return case_label

    def get_resource_label(self, resource_id):
        resource_label = str(resource_id)
        return resource_label
