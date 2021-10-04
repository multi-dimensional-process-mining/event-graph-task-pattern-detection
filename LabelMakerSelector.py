from labelmakers.GeneralLabelMaker import GeneralLabelMaker
from labelmakers.BPIC2017LabelMaker import BPIC2017LabelMaker
from labelmakers.BPIC2014LabelMaker import BPIC2014LabelMaker


def get_label_maker(name_data_set, use_label_dict=False, activity_label="activity", print_duration=False,
                    activity_label_sep=" "):
    if name_data_set == "bpic2017":
        return BPIC2017LabelMaker(use_label_dict=use_label_dict, activity_label=activity_label,
                                  print_duration=print_duration, activity_label_sep=activity_label_sep)

    elif name_data_set == "bpic2014":
        return BPIC2014LabelMaker(activity_label=activity_label, print_duration=print_duration,
                                  activity_label_sep=activity_label_sep)

    else:
        return GeneralLabelMaker(activity_label=activity_label, print_duration=print_duration,
                                 activity_label_sep=activity_label_sep)
