CICIDS_MAPPING = {
    'Source IP': 'src_ip',
    'Destination IP': 'dst_ip',
    'Source Port': 'src_port',
    'Destination Port': 'dst_port',
    'Protocol': 'protocol',
    'Label': 'label'
}


# Os quatro CSVs completos do UNSW-NB15 não possuem cabeçalho. Somente as
# colunas necessárias à reconstrução dos rótulos são carregadas.
UNSW_NB15_LABEL_USECOLS = [0, 1, 2, 3, 4, 28, 29, 47, 48]

UNSW_NB15_LABEL_COLUMNS = [
    "src_ip",
    "src_port",
    "dst_ip",
    "dst_port",
    "protocol_name",
    "start_time_s",
    "last_time_s",
    "attack_category",
    "source_binary_label",
]
