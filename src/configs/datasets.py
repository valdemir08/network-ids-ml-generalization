from pathlib import Path

DATASETS = {

    "cicids2017": {

        "root": Path(r"E:/datasets/cicids2017"),

        "flow_output_mode": "scenario",
        "matching_time_tolerance_ms": 60_000,

        "pcap_dir": "pcaps",
        "label_dir": "csvs",

        "scenarios": {

            "friday": {

                "pcaps": [
                    "Friday-WorkingHours.pcap"
                ],

                "labels": [
                    "Friday-WorkingHours-Morning.pcap_ISCX.csv",
                    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
                    "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"
                ]
            },

            "monday": {

                "pcaps": [
                    "Monday-WorkingHours.pcap"
                ],

                "labels": [
                    "Monday-WorkingHours.pcap_ISCX.csv",
                ]
            },

            "thursday": {

                "pcaps": [
                    "Thursday-WorkingHours.pcap"
                ],

                "labels": [
                    "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv",
                    "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
                ]
            },

            "tuesday": {

                "pcaps": [
                    "Tuesday-WorkingHours.pcap"
                ],

                "labels": [
                    "Tuesday-WorkingHours.pcap_ISCX.csv",
                ]
            },

            "wednesday": {

                "pcaps": [
                    "Wednesday-WorkingHours.pcap"
                ],

                "labels": [
                    "Wednesday-workingHours.pcap_ISCX.csv",
                ]
            },

        }
    },

    "unsw_nb15": {

        "root": Path(r"E:/datasets/unsw"),

        "flow_output_mode": "per_pcap",
        "matching_time_tolerance_ms": 1_000,

        "pcap_dir": "pcaps",
        "label_dir": "csvs",
        "label_time_margin_ms": 300_000,
        "ground_truth_file": "NUSW-NB15_GT.csv",
        "comparison_flow_file": "CICFlowMeter_out.csv",

        "scenarios": {

            "2015_01_22": {

                "pcaps": [
                    f"22-1-2015/{index}.pcap"
                    for index in range(1, 54)
                ],

                "labels": [
                    "UNSW-NB15_1.csv",
                    "UNSW-NB15_2.csv",
                ]
            },

            "2015_02_17": {

                "pcaps": [
                    f"17-2-2015/{index}.pcap"
                    for index in range(1, 28)
                ],

                "labels": [
                    "UNSW-NB15_2.csv",
                    "UNSW-NB15_3.csv",
                    "UNSW-NB15_4.csv",
                ]
            },

        },
    },

    "iot23": {

        "root": Path(r"E:/datasets/iot23"),

        "flow_output_mode": "chunked",
        "flow_chunk_size": 250_000,
        "matching_time_tolerance_ms": 1,

        "pcap_dir": ".",
        "label_dir": ".",

        "pipeline_scenarios": [
            "honeypot_4_1",
            "honeypot_5_1",
            "honeypot_7_1",
            "malware_1_1",
            "malware_3_1",
            "malware_8_1",
            "malware_20_1",
            "malware_21_1",
            "malware_34_1",
            "malware_35_1",
            "malware_42_1",
        ],

        "scenarios": {

            "honeypot_4_1": {
                "pcaps": [
                    "CTU-Honeypot-Capture-4-1/"
                    "2018-10-25-14-06-32-192.168.1.132.pcap"
                ],
                "labels": [
                    "CTU-Honeypot-Capture-4-1/bro/conn.log.labeled"
                ],
            },

            "honeypot_5_1": {
                "pcaps": [
                    "CTU-Honeypot-Capture-5-1/2018-09-21-capture.pcap"
                ],
                "labels": [
                    "CTU-Honeypot-Capture-5-1/bro/conn.log.labeled"
                ],
            },

            "honeypot_7_1": {
                "pcaps": [
                    "CTU-Honeypot-Capture-7-1/Somfy-01/"
                    "2019-07-03-15-15-47-first_start_somfy_gateway.pcap"
                ],
                "labels": [
                    "CTU-Honeypot-Capture-7-1/Somfy-01/"
                    "bro/conn.log.labeled"
                ],
            },

            "malware_1_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-1-1/"
                    "2018-05-09-192.168.100.103.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-1-1/bro/conn.log.labeled"
                ],
            },

            "malware_3_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-3-1/2018-05-21_capture.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-3-1/bro/conn.log.labeled"
                ],
            },

            "malware_7_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-7-1/"
                    "2018-07-20-17-31-20-192.168.100.108.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-7-1/bro/conn.log.labeled"
                ],
            },

            "malware_8_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-8-1/"
                    "2018-07-31-15-15-09-192.168.100.113.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-8-1/bro/conn.log.labeled"
                ],
            },

            "malware_9_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-9-1/"
                    "2018-07-25-10-53-16-192.168.100.111.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-9-1/bro/conn.log.labeled"
                ],
            },

            "malware_17_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-17-1/"
                    "2018-09-06-11-43-12-192.168.100.111.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-17-1/bro/conn.log.labeled"
                ],
            },

            "malware_20_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-20-1/"
                    "2018-10-02-13-12-30-192.168.100.103.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-20-1/bro/conn.log.labeled"
                ],
            },

            "malware_21_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-21-1/"
                    "2018-10-03-15-22-32-192.168.100.113.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-21-1/bro/conn.log.labeled"
                ],
            },

            "malware_33_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-33-1/"
                    "2018-12-20-21-10-00-192.168.1.197.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-33-1/bro/conn.log.labeled"
                ],
            },

            "malware_34_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-34-1/"
                    "2018-12-21-15-50-14-192.168.1.195.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-34-1/bro/conn.log.labeled"
                ],
            },

            "malware_35_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-35-1/"
                    "2018-12-21-15-33-59-192.168.1.196.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-35-1/bro/conn.log.labeled"
                ],
            },

            "malware_36_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-36-1/"
                    "2018-12-21-13-36-41-192.168.1.198.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-36-1/bro/conn.log.labeled"
                ],
            },

            "malware_39_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-39-1/"
                    "2019-01-09-21-25-11-192.168.1.194.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-39-1/bro/conn.log.labeled"
                ],
            },

            "malware_42_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-42-1/"
                    "2019-01-10-14-34-38-192.168.1.197.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-42-1/bro/conn.log.labeled"
                ],
            },

            "malware_43_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-43-1/"
                    "2019-01-10-19-22-51-192.168.1.198.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-43-1/bro/conn.log.labeled"
                ],
            },

            "malware_44_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-44-1/"
                    "2019-01-10-21-06-26-192.168.1.199.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-44-1/bro/conn.log.labeled"
                ],
            },

            "malware_48_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-48-1/"
                    "2019-02-28-19-15-13-192.168.1.200.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-48-1/bro/conn.log.labeled"
                ],
            },

            "malware_49_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-49-1/"
                    "2019-02-28-20-50-15-192.168.1.193.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-49-1/bro/conn.log.labeled"
                ],
            },

            "malware_52_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-52-1/"
                    "2019-03-08-13-24-30-192.168.1.197.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-52-1/bro/conn.log.labeled"
                ],
            },

            "malware_60_1": {
                "pcaps": [
                    "CTU-IoT-Malware-Capture-60-1/"
                    "2019-09-20-02-40-32-192.168.1.195.pcap"
                ],
                "labels": [
                    "CTU-IoT-Malware-Capture-60-1/bro/conn.log.labeled"
                ],
            },

        },
    }

}
