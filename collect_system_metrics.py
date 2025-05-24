import subprocess
import os
import re
from sys import stdout
from time import sleep

class Collect_System_Metrics:
    def __init__(self):
        super().__init__()
        self.name = "System  Data Collector"
        self.__DCS_NAME = "system_stream"
        dcs_not_exists = True
        try:
            output = subprocess.check_output(["logman", "query"], text=True)
            dcs_not_exists = (self.__DCS_NAME not in output)
        except Exception as e:
            dcs_not_exists = False
            print(e)

        if dcs_not_exists:
            try:
                all_network_interface_list = subprocess.check_output(["typeperf", "-qx", r"\Network Interface"], text=True)
                current_bandwidth_interfaces = re.findall(r'\\Network Interface\((.*?)\)\\Current Bandwidth', all_network_interface_list)
                wifi_interface = None
                for interface in current_bandwidth_interfaces:
                    iface = interface.lower()
                    if "wi-fi" in iface:
                        wifi_interface = interface
                    elif "wifi" in iface:
                        wifi_interface = interface
                    elif "802.11ac" in iface:
                        wifi_interface = interface

                if wifi_interface:
                    wifi_counter = fr"\Network Interface({wifi_interface})\Current Bandwidth"
                else:
                    print("Wi-Fi interface not found... Using wildcard (*) instead.")
                    wifi_counter = r"\Network Interface(*)\Current Bandwidth"

            except Exception as e:
                print("Error detecting network interfaces:", e)
                wifi_counter = r"\Network Interface(*)\Current Bandwidth"


            LOG_OUTPUT_DIR  = r".\NET_SYS\SYSTEM"
            SAMPLE_INTERVAL = "00:00:01"
            counters =  [
                            r"\Memory\% Committed Bytes In Use",
                            wifi_counter,
                            r"\PhysicalDisk(_Total)\% Disk Time",
                            r"\PhysicalDisk(_Total)\Disk Reads/sec",
                            r"\PhysicalDisk(_Total)\Disk Writes/sec",
                            r"\PhysicalDisk(_Total)\Current Disk Queue Length",
                            r"\Processor(_Total)\% Processor Time",
                            r"\Processor Information(_Total)\% Processor Performance",
                            r"\Processor Information(_Total)\% Processor Utility",
                            r"\Processor Information(_Total)\% Processor Time"
                        ]

            cmd = [ "logman", "create", "counter", self.__DCS_NAME,
                    "-c", *counters,
                    "-si", SAMPLE_INTERVAL,
                    "-f", "csv", 
                    "-o", os.path.join(LOG_OUTPUT_DIR, self.__DCS_NAME), 
                    "-a"
                  ]

            try:
                subprocess.run(cmd, check=True)
                print(f" => Created System Metrics collector : {self.__DCS_NAME}")
                print("!!!!\n\tPress Win > Search `PerfMon` tool > \n\tData Collector Sets > User Defined > system_stream > system_stream (double-click) > \n\tFile > File name format > Select <none> & click Apply, then Ok.\n\n\tIf Done, wait until countdown.. If not Done, you will get errors...\n!!!!\n")
                for i in range(60,-1,-1):
                    print(f"\r...{i}s...",end="")
                    stdout.flush()
                    sleep(1)
                print("\n\n")
            except:
                pass

    def start(self):
        self.run()

    def run(self):
        try:
            subprocess.run(["logman", "start", self.__DCS_NAME], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            self.stop_collection()
            print(e)

    def stop_collection(self):
        try:
            subprocess.run(["logman", "stop", self.__DCS_NAME], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(e)
