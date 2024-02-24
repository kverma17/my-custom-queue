from custom_queue import CustomQueue

NO_OF_MSG_TO_SEND = 10

def send_msg_to_queue(no_of_msgs):
    my_queue = CustomQueue()
    while no_of_msgs > 0:
        message_id = NO_OF_MSG_TO_SEND - no_of_msgs + 1
        my_msg = {"messageId": f"uuid_{message_id}"}
        my_queue.add_message(my_msg)
        print(f"Message number {message_id} sent from producer")
        no_of_msgs -= 1

if __name__ == "__main__":
    send_msg_to_queue(NO_OF_MSG_TO_SEND)
