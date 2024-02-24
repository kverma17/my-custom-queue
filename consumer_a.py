import threading
import traceback
from time import sleep
from custom_queue import CustomQueue

class ConsumerA:

    def __init__(self, sleep_time = 1):
        self.sleep_time = sleep_time

    def consumer_a(self):
        my_queue = CustomQueue()
        while True:
            try:
                msg = my_queue.read_message()
                if msg:
                    print(f"ConsumerA consumed messageId: {msg.get('messageId')}")
            except Exception as _exp:
                print(f"Error in reading msg from queue.")
                print(f"Exception: {_exp}")
                print(traceback.format_exc())
            sleep(self.sleep_time)

    def consumer_b(self):
        my_queue = CustomQueue()
        while True:
            try:
                msg = my_queue.read_message()
                if msg:
                    print(f"ConsumerB consumed messageId: {msg.get('messageId')}")
            except Exception as _exp:
                print(f"Error in reading msg from queue.")
                print(f"Exception: {_exp}")
                print(traceback.format_exc())
            sleep(self.sleep_time)


if __name__ == "__main__":
    consumer_obj = ConsumerA()

    thread_a = threading.Thread(target=consumer_obj.consumer_a)
    thread_b = threading.Thread(target=consumer_obj.consumer_b)

    thread_a.start()
    thread_b.start()

    thread_a.join()
    thread_b.join()
