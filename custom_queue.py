import json
import threading
from time import time

class CustomQueue:
    def __init__(self, data_file='queue_data.json', meta_file='meta_data.json'):
        self.data_file = data_file
        self.meta_file = meta_file
        self.lock = threading.RLock()
        self.load_data()
        self.load_meta_data()

    def load_data(self):
        try:
            with self.lock:
                with open(self.data_file, 'r') as file:
                    self.queue_data = json.load(file)
        except FileNotFoundError:
            self.queue_data = []

    def load_meta_data(self):
        try:
            with self.lock:
                with open(self.meta_file, 'r') as file:
                    self.meta_data = json.load(file)
        except FileNotFoundError:
            self.meta_data = {}

    def save_data(self):
        with self.lock:
            with open(self.data_file, 'w') as file:
                json.dump(self.queue_data, file)

    def save_meta_data(self):
        with self.lock:
            with open(self.meta_file, 'w') as file:
                json.dump(self.meta_data, file)

    def add_message(self, data: dict):
        epoch_time = int(time())
        self.queue_data.append(data)
        msg_index = len(self.queue_data) - 1
        self.meta_data[msg_index] = epoch_time  # Save the index of the added message
        self.save_data()
        self.save_meta_data()
        print(f"New message {data} added to the queue, Queue size: {len(self.queue_data)}")

    def read_message(self):
        with self.lock:
            current_time = int(time())
            if not self.meta_data:
                print("Queue is empty. Cannot read a message.")
                return None

            last_index = max([int(i) for i in self.meta_data.keys()])
            last_message = self.queue_data.pop(last_index)
            last_message_epoch_time = self.meta_data[str(last_index)]
            del self.meta_data[str(last_index)]
            self.save_data()
            self.save_meta_data()

            time_difference = current_time - last_message_epoch_time

            if time_difference <= 10:
                print(f"Read message: {last_message}")
                return last_message
            else:
                print(f"Message ID {last_index} is expired.")
                return None
