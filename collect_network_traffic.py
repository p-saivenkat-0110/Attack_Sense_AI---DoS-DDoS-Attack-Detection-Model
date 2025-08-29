import subprocess
import os
import threading
import pandas as pd
from time import sleep

class Collect_Network_Traffic(threading.Thread):
    def __init__(self, shutdown_event):
        super().__init__()
        self.shutdown_event = shutdown_event
        self.daemon = True
        self.name = "Network Data Collector"
        self.__CICFLOWMETER = r'.\CICFlowMeter-4.0\bin\cfm.bat'
        self.__CAPTURE_INTERFACE = 'Wi-Fi'
        self.__CAPTURE_DURATION = 5
        self.__OUTPUT_DIR = r".\NET_SYS\NETWORK"
        os.makedirs(self.__OUTPUT_DIR, exist_ok=True)
        self.__pcap = os.path.join(self.__OUTPUT_DIR, "stream.pcap")
        self.__csv  = os.path.join(self.__OUTPUT_DIR, "network_stream.csv")

    def __capture_network_data_through_tshark(self):
        command  =  [
                        'tshark', '-i', self.__CAPTURE_INTERFACE,
                        '-a', f'duration:{self.__CAPTURE_DURATION}',
                        '-F', 'pcap',
                        '-w', self.__pcap
                    ]
        try:
            subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(e)

    def __convert_pcap_to_csv_through_cicflowmeter(self):
        command = [self.__CICFLOWMETER, self.__pcap, self.__OUTPUT_DIR]
        try:
            subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.__append_to_network_stream()
        except Exception as e:
            print(e)

    def __append_to_network_stream(self):
        try:
            generated_csv = None
            for file in os.listdir(self.__OUTPUT_DIR):
                if file.endswith(".csv") and file != os.path.basename(self.__csv):
                    generated_csv = os.path.join(self.__OUTPUT_DIR, file)
                    break
            if generated_csv and os.path.isfile(generated_csv):
                df = pd.read_csv(generated_csv)
                if os.path.isfile(self.__csv):
                    df.to_csv(self.__csv, mode='a', header=False, index=False)
                else:
                    df.to_csv(self.__csv, index=False)
                os.remove(generated_csv)
        except Exception as e:
            print(e)

    def run(self):
        while not self.shutdown_event.is_set():
            try:
                self.__capture_network_data_through_tshark()
                self.__convert_pcap_to_csv_through_cicflowmeter()
            except Exception as e:
                self.stop_collection()
                print(e)
    
    def stop_collection(self):
        self.__append_to_network_stream()
        sleep(1)
        if os.path.exists(self.__pcap) : os.remove(self.__pcap)
        if os.path.exists(self.__csv)  : os.remove(self.__csv)