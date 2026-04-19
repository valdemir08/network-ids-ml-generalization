from pathlib import Path

DATASETS = {

    "cicids2017": {

        "root": Path(r"E:/datasets/cicids2017"),

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
    }

}
