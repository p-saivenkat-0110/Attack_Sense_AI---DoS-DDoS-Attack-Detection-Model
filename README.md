# Attack Sense AI - DoS & DDoS Attack Detection Model

> **Associated Paper:** *Beyond Packets: Unifying System Metrics and Network Traffic for DoS and DDoS Attack Detection (Under Review)*  
> **Dataset:** HyNetSys — available on IEEE DataPort at [https://dx.doi.org/10.21227/xn7t-jj98](https://dx.doi.org/10.21227/xn7t-jj98)

---

## Overview

Most intrusion detection systems monitor either network traffic or host-level system metrics but not both. **AttackSense AI** bridges that gap by fusing network flow features with system performance indicators (CPU, memory, disk I/O) into a unified temporal representation, enabling detection of attack patterns that would remain invisible to either source alone.

Detection is performed using **Gated Recurrent Unit (GRU)** models trained across multiple time windows (1–5 minutes), wrapped in a **pipelined queue-based architecture** that raises early alerts for obvious attacks while reserving deeper temporal analysis for subtler threats, balancing detection speed with classification accuracy.

The best-performing configuration achieved a peak classification accuracy of **99.80%** across stratified 5-fold cross-validation on the HyNetSys dataset.

---

## Dataset - HyNetSys

HyNetSys is a purpose-built hybrid dataset collected under controlled Normal, DoS and DDoS operating conditions using a VirtualBox-based network environment (Kali Linux, Windows, Ubuntu).

| Split | Network Samples | System Samples |
|---|---|---|
| Normal | 1,34,948 | 45,630 |
| DoS | 1,33,411 | 38,574 |
| DDoS | 1,31,974 | 45,788 |
| **Total** | **4,00,333** | **1,29,992** |

- **Network features (84):** Extracted from PCAP files using CICFlowMeter - packet statistics, flow duration, inter-arrival times, flag counts, port and protocol information.
- **System features (10):** Collected via Windows Performance Monitor - CPU utilisation, memory usage, disk reads/writes, disk queue length, network bandwidth.

> Download the dataset: [https://dx.doi.org/10.21227/xn7t-jj98](https://dx.doi.org/10.21227/xn7t-jj98)

---

## Key Contributions

- **Hybrid Feature Fusion** - Joint modelling of system performance metrics and network flow features, treating them as complementary rather than competing signals.
- **Time-Aligned Preprocessing** - Synchronisation of heterogeneous data sources operating at different resolutions (millisecond network vs. second-level system data) via timestamp-based aggregation.
- **Robust Preprocessing Pipeline** - Missing value imputation via linear interpolation, infinite value capping at the 99th percentile, log transformation, Min-Max normalisation (network) and Z-score standardisation (system) and removal of 14 constant features.
- **Multi-Scale Temporal Windowing** - Data segmented into 1–5 minute windows to capture both rapid volumetric attacks and slower, more subtle intrusion patterns.
- **Pipelined Detection Architecture** - Sequential evaluation through GRU models of increasing window size. An attack flagged at any stage triggers an immediate alert without waiting for subsequent stages. Two variants are provided:
  - **Comprehensive Window Pipeline (CWP):** Windows of 1, 2, 3, 4 and 5 minutes.
  - **Selective Window Pipeline (SWP):** Lightweight variant using 1, 3 and 5 minutes only.

---

## Requirements

### System Dependencies

**1. Wireshark**
- During installation, check **Tshark** and **Npcap**
- Install **Npcap**
- Add the Wireshark installation directory to the `PATH` user environment variable

**2. Java Development Kit (JDK 21)**
- Download from [https://www.oracle.com/java/technologies/downloads/](https://www.oracle.com/java/technologies/downloads/)
- Ensure `JAVA_HOME` is set and `java` is accessible from the system PATH

**3. CICFlowMeter**
- Install from: [https://github.com/ahlashkari/CICFlowMeter](https://github.com/ahlashkari/CICFlowMeter)
- Build the package (`mvn package`) and copy the output package into this project's root directory

**4. Python Dependencies**
```bash
pip install -r requirements.txt
```
> If dependency conflicts arise, use **Python 3.11.0**.

---

## Getting Started

1. Clone this repository:
```bash
git clone https://github.com/p-saivenkat-0110/Attack_Sense_AI---DoS-DDoS-Attack-Detection-Model.git
cd Attack_Sense_AI---DoS-DDoS-Attack-Detection-Model
```

2. Install all dependencies as described above.

3. Open a terminal **as Administrator** and run:
```bash
python main.py
```

4. Follow the on-screen instructions to configure the detection pipeline.

---

## Citation

If you use this code or the HyNetSys dataset in your research, please cite:

```
Tata Umesh, Peruri Sai Venkat, Pulukuri Shalem Vikranth, Narendran Rajagopalan,
"Beyond Packets: Unifying System Metrics and Network Traffic for DoS and DDoS Attack Detection",
manuscript under review.
```

Dataset citation:
```
Umesh Tata, Sai Venkat Peruri, Shalem Vikranth Pulukuri, Narendran Rajagopalan, "HyNetSys: Hybrid Network and System Dataset for DoS and DDoS Attack Detection", IEEE Dataport, April 22, 2025, doi:10.21227/xn7t-jj98
```

> *This section will be updated with full journal details upon acceptance.*

---

## Issues and Contributions

Found a bug or have a question? Please [open an issue](https://github.com/p-saivenkat-0110/Attack_Sense_AI---DoS-DDoS-Attack-Detection-Model/issues) and we will follow up.

---

## License

This project is made publicly available to facilitate reproducibility, transparency and future extensions of this work by the research community.
