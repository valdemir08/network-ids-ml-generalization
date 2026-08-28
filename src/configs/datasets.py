from pathlib import Path

DATASETS = {

    "cicids2017": {

        "root": Path(r"E:/datasets/cicids2017"),

        "flow_output_mode": "scenario",

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

        }
    }

}
