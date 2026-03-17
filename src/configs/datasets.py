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
            }

        }
    }

}