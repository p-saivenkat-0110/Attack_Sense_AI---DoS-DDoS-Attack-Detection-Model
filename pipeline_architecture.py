import os,threading
import tensorflow as tf
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import numpy as np
from time import sleep
from plyer import notification

class Model:
    def __init__(self, model_path):
        self.model = tf.keras.models.load_model(model_path)

    def predict(self, window_data):
        window_data = window_data.reshape((1, window_data.shape[0], window_data.shape[1]))
        prediction = self.model.predict(window_data, verbose=0)
        return np.argmax(prediction)

class Parallel_Executer(threading.Thread):
    def __init__(self, name, model, data_loader, queue_in, queue_out, window_size, shutdown_event):
        super().__init__()
        self.shutdown_event = shutdown_event
        self.name = name
        self.model = model
        self.data_loader = data_loader
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.window_size = window_size
        self.daemon = True
        print(f"-> {self.name} initialized...")

    def __notify(self, timestamp, attackType):
        print(f"\033[1;31m[{self.name}] : ALERT!! {attackType} detected at {timestamp}\033[1;0m")
        notification.notify(
                    title=f"⚠️ {attackType} Attack Detected!!",
                    message=f"{self.name} model detected attack at {timestamp}",
                    timeout=5
                )
        sleep(0.1)
        self.shutdown_event.set()

    def run(self):
        while not self.shutdown_event.is_set():
            if self.queue_in.empty():
                continue
            timestamp = self.queue_in.get()
            window_input = self.data_loader.fetch_past_K_minute_data(timestamp, self.window_size)
            if window_input.shape[0] == 0:
                continue

            pred = self.model.predict(window_input)
            if pred == 1:
                self.__notify(timestamp, "DOS")
                continue
            elif pred == 2:
                self.__notify(timestamp, "DDOS")
                continue
            else:
                if self.queue_out:
                    print(f"[{self.name}] : Normal → Passing to next queue")
                    self.queue_out.put(timestamp)
                else:
                    print(f"[{self.name}] : Normal -- Passed all checks !!")