"""
colunas que não devem ser usadas no ML, não necessitam passar por análise
"""

DROP_COLUMNS = {
    # identificadores
    # expiration_id
    # nfdoc: Identifier of flow expiration trigger. Can be 0 for idle_timeout, 1 for active_timeout or -1 for custom expiration.
    # é artefato do nfstream e não indica comportamento de rede
    #  dependem dos parâmetros citados em sua descrição
    "id",
    "expiration_id",
    "flow_key",
    "flow_key_rev",

    # timestamps absolutos (totalmente relacionado ao momento de captura dos experimentos)
    "bidirectional_first_seen_ms",
    "bidirectional_last_seen_ms",
    "src2dst_first_seen_ms",
    "src2dst_last_seen_ms",
    "dst2src_first_seen_ms",
    "dst2src_last_seen_ms",

    # identificadores de rede
    # src_ip, dst_ip -> máquinas específicas do experimento
    # src_mac, dst_mac -> identificador único das máquinas do experimento
    # src_oui, dst_oui -> identifica o fabricante das máquinas (parecidop com mac)
    "src_ip",
    "dst_ip",
    "src_mac",
    "dst_mac",
    "src_oui",
    "dst_oui",

    # strings de altíssima cardinalidade / pouco valor geral / muitos valores nulos
    # o modelo não deve generalizar comportamento e não se prender a dados relacionados ao experimento/dataset

    # porcentagem de valores NULOS (cicids2017)
    # requested_server_name          50.34
    # client_fingerprint             89.91
    # server_fingerprint             89.98
    # user_agent                     89.96
    # content_type                   87.22

    # "requested_server_name", -> nfdoc: Requested server name (SSL/TLS, DNS, HTTP).
    # ex: vast.bp3854372.btrll.com, static.ilcdn.fi, wildcard.moatads.com.edgekey.net, log1.17173.com ....
    # indica destino, não comportamento de rede

    # "client_fingerprint", -> nfdoc: Client fingerprint (DHCP fingerprint for DHCP, JA4 for SSL/TLS and HASSH for SSH).
    # ex: t12d1510h2_073e58a039a6_b44afb9f0e6a
    # específico do dataset/experimento. um hash que não informa

    # "server_fingerprint", -> 	nfdoc: Server fingerprint (JA3 for SSL/TLS and HASSH for SSH).
    # ex: 18e962e106761869a61045bed0e81c2c
    # específico do dataset/experimento. um hash que não informa

    # "user_agent", -> nfdoc: Extracted user agent for HTTP or User Agent Identifier for QUIC.
    # ex: Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.1) Gecko/20090718 Firefox/3.5.1
    # alta cardinalidade (é texto livre)

    # "content_type", -> nfdoc: Extracted HTTP content type.
    # ex: application/ocsp-response, text/html ...
    # até tem uma cardinalidade mediana, mas o maior motivo para remoção é a grande quantidade de valores nulos

    "requested_server_name",
    "client_fingerprint",
    "server_fingerprint",
    "user_agent",
    "content_type",


    # identifica apenas a origem do flow
    "dataset_name",

    # vlan_id -> Virtual LAN identifier.
        # totalmente atrelado a rede local, não generaliza
    # tunnel_id ->  Tunnel identifier (O: No Tunnel, 1: GTP, 2: CAPWAP, 3: TZSP).
        # depende do contexto da rede (ex: GTP altamente ligado a redes móveis).
        # está relacionada à infraestrutura de rede e não ao comportamento do tráfego.
        # em cenários onde todos os fluxos compartilham o mesmo tipo de túnel, a feature perde poder discriminativo.
        # modelo aprende características do contexto ao invés do comportamento
        # do tráfego, prejudicando a generalização entre datasets.
    "vlan_id",
    "tunnel_id",

    # "application_name", -> nDPI detected application name.
        # nome da aplicação. ex: TLS, TLS.Twitter, DNS.Mozilla, DNS.Google .....
    # "application_category_name", ->n DPI detected application category name.
        # categoria da aplicação. ex: Web, Network, Advertisement ....
    # features inconsistentes, atreladas aos datasets
    "application_name",
    "application_category_name",

    #"application_is_guessed" -> Indicates if detection result is based on pure dissection or on a guess heuristics.
        # essa feature indica se o nfstream conseguiu identificar claramente a aplicação com base em assinaturas, protocolos ...
        # nome e categoria da aplicação não interessam para os modelos, por consequência isso aqui também não é importante
        # 0 -> identificado com certeza
        # 1 -> provavél
    "application_is_guessed",
    #"application_confidence",-> Indicates the underlying detection method (O: Unknown classification, 1: Classification obtained looking only at the L4 ports, 3: Classification results based on partial/incomplete DPI information, 4: Classification results based on some LRU cache with partial/incomplete DPI information, 5: Classification results based on some LRU cache (i.e. correlation among sessions), 6: Deep packet inspection).
        # indica confiança na classificação da aplicação, mas também não tem utilidade pelo mesmo motivo que "application_is_guessed"
    "application_confidence",


    # removidas por baixo impacto / irrelenvância no modelo abaixo


}



CATEGORICAL_COLUMNS = {
    "protocol",
    "src_port",
    "dst_port",
    "ip_version",
}

TARGET_COLUMN = "label"


def split_features(df):
    numeric_cols = df.select_dtypes(include='number').columns.tolist()

    valid_cols = [col for col in df.columns if col not in DROP_COLUMNS and col != TARGET_COLUMN]

    # categóricas incluindo as interpretadas como numéricas
    categorical = [
        col for col in valid_cols
        if col in CATEGORICAL_COLUMNS
    ]

    # numéricas reais
    numeric = [
        col for col in numeric_cols
        if col in valid_cols and col not in CATEGORICAL_COLUMNS
    ]

    return numeric, categorical
