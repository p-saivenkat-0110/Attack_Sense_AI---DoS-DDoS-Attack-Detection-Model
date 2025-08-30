# Attack Sense AI - DoS/DDoS Attack Detection

This repository provides the codebase used in the research study associated with the dataset **HyNetSys: Hybrid Network and System Dataset for DoS and DDoS Attack Detection**.

The dataset is available at:  
https://dx.doi.org/10.21227/xn7t-jj98

This repository is linked to our journal submission titled:  
**Beyond Packets: Unifying System Metrics and Network Traffic for DoS and DDoS Attack Detection**

**About the Project**

HyNetSys combines system performance metrics (CPU, memory, disk I/O) with network flow features (extracted using CICFlowMeter from packet captures) to detect and classify cyberattacks with high accuracy. To balance early detection and overall accuracy, a pipelined architecture is employed. This approach sequentially evaluates data using models trained on multiple time windows (from shortest to longest). If an attack is detected at any stage, it is immediately flagged and mitigated, avoiding further computation. This design ensures fast response in real-time scenarios while preserving deep analysis capabilities through larger-window models.

**Key contributions**
- _Hybrid Feature Space_: Fusion of system metrics with network flow features.
- _Time-Aligned Fusion_: Synchronization of system and network data using timestamp alignment.
- _Robust Preprocessing_: Missing value imputation, data smoothing, feature selection, and normalization.
- _Temporal Windowing_: Aggregation into 1–5 minute windows to capture sequential attack patterns.
- _Deep Learning Models_: Training and evaluation of deep learning architectures.
- _Pipelined Architecture for Early and Accurate Detection_: To address the trade-off between accuracy (higher with longer windows) and timeliness (faster with shorter windows), a pipelined evaluation strategy was developed.

# Requirements
1) **Wireshark**
   - Checkbox Tshark, Npcap
   - Install Npcap
   - Add `Wireshark` location to user variables PATH
2) **Install JDK 21(Java Development Kit)**
   - Make sure java is added to system environment variables
3) **CICFLOWMETER** (install from here : https://github.com/ahlashkari/CICFlowMeter)
   - Make package and copy that package in this folder
4) **pip install -r requirements.txt**

5) *(Optional) If any dependency issue occurs, use python 3.11.0*

!!! *Any doubts/errors, please create an issue so we could follow up on it...* !!!

# How to Run?
- Open terminal as Administrator & change directory to this project.
- run `python main.py`
-- ***follow steps as instructed by program***
