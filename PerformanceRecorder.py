import pandas as pd
import time


class PerformanceRecorder:

    def __init__(self, data_set, description):
        self.performance_table = pd.DataFrame(columns=['action', 'start', 'end', 'duration'])
        self.start = time.time()
        self.last = time.time()
        self.path_to_performance_file = f'performance/{description}_{data_set}.csv'
        print(f"START {description}...")

    def start_recording(self):
        self.last = time.time()

    def record_performance(self, action):
        current_time = time.time()
        self.performance_table = self.performance_table.append({'action': action,
                                                                'start': self.last, 'end': current_time,
                                                                'duration': (current_time - self.last)},
                                                               ignore_index=True)
        print(action + ' done -  took ' + str(current_time - self.last) + ' seconds')
        self.last = current_time

    def record_total_performance(self):
        current_time = time.time()
        self.performance_table = self.performance_table.append({'action': 'total',
                                                                'start': self.start, 'end': current_time,
                                                                'duration': (current_time - self.start)},
                                                               ignore_index=True)
        print('total' + ' done -  took ' + str(current_time - self.start) + ' seconds\n')
        self.last = current_time

    def save_to_file(self):
        self.performance_table.to_csv(self.path_to_performance_file)
