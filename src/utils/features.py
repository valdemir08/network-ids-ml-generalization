"""
colunas que não devem ser usadas no ML, não necessitam passar por análise
"""

import numpy as np
import pandas as pd

columns_to_ignore = {
    # identificadores
    # expiration_id
    # nfdoc: Identifier of flow expiration trigger. Can be 0 for idle_timeout, 1 for active_timeout or -1 for custom expiration.
    # é artefato do nfstream e não indica comportamento de rede
    #  dependem dos parâmetros citados em sua descrição
    "id",
    "expiration_id",
    "flow_key",
    "flow_key_rev",
    # metadados do processo de rotulagem; não descrevem o tráfego e
    # provocariam vazamento de informação se fossem usados como features
    "label_binary",
    "label",
    "match_status",
    "match_direction",
    "match_time_diff_ms",

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

    # porcentagem de valores NULOS
    #
    # (cicids2017)
    #server_fingerprint             90.18
    #client_fingerprint             90.10
    #user_agent                     89.05
    #content_type                   85.60
    #requested_server_name          51.13

    #(unsw)
    # server_fingerprint             97.95
    # client_fingerprint             97.94
    # content_type                   91.30
    # user_agent                     87.66
    # requested_server_name          65.63

    #(iot23)
    # content_type                   99.99
    # user_agent                     99.99
    # server_fingerprint             99.99
    # client_fingerprint             99.99
    # requested_server_name          99.81

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



# features constantes ou quase constantes


const_features = {
    #constante nos 3 datasets
    "dst2src_urg_packets",
}

sparse_tcp_features = {
    # Constantes em ao menos uma base de treino e presentes em menos
    # de 0,2% dos fluxos nas demais.
    "bidirectional_cwr_packets",
    "bidirectional_ece_packets",
    "bidirectional_urg_packets",
    "src2dst_cwr_packets",
    "src2dst_ece_packets",
    "src2dst_urg_packets",
    "dst2src_cwr_packets",
    "dst2src_ece_packets",
}

# colunas ignoradas
# alta correlação + redundância

# features direcionais src2dst / dst2src dependem da orientação do fluxo,
# que é definida pelo primeiro pacote observado no nfstream.
# essa orientação pode ser inconsistente devido a cortes por timeout ou início da captura
# não há garantia do comportamento da feature.
# para reduzir viés direcional,
# as features direcionais são removidas quando existe equivalente bidirecional.


identical_features = {
    # na análise de correlação ...
    ### Direção

    #['bidirectional_urg_packets', 'src2dst_urg_packets']
    "src2dst_urg_packets",

    #("bidirectional_packets", "src2dst_packets"),
    #("bidirectional_packets", "dst2src_packets"),
    #("src2dst_packets", "dst2src_packets"),
    "src2dst_packets",
    "dst2src_packets",

    #("bidirectional_duration_ms", "src2dst_duration_ms"),
    #("bidirectional_duration_ms", "dst2src_duration_ms"),
    #("src2dst_duration_ms", "dst2src_duration_ms"),

    # as 2 features de caminho possuem distribuição idêntica,
    # src2dst/dst2src duration_ms
        # representam informações distintas
        # mas, possuem distribuição idêntica, e
        # podem enviesar os modelos por conta da direção
    "src2dst_duration_ms",
    "dst2src_duration_ms",

    #("bidirectional_ack_packets", "src2dst_ack_packets"),
    #("bidirectional_ack_packets", "dst2src_ack_packets"),
    #("src2dst_ack_packets", "dst2src_ack_packets"),
    "src2dst_ack_packets",
    "dst2src_ack_packets",

    #("bidirectional_ack_packets", "src2dst_ack_packets"),
    #("bidirectional_ack_packets", "dst2src_ack_packets"),
    #("src2dst_ack_packets", "dst2src_ack_packets"),
    #("bidirectional_packets", "bidirectional_ack_packets"),
    # redundante me tcp já que praticmaente tod o pacote tcp tem uma confirmação "ack". sempre 0 em udp
    # pode acabar virando atalho para protocolo. (verificar o que fazer com protocolo, já que é categórica)
    "bidirectional_ack_packets",
    "src2dst_ack_packets",
    "src2dst_packets",



    #("bidirectional_cwr_packets", "src2dst_cwr_packets"),
    #("bidirectional_ece_packets", "dst2src_ece_packets"),
    # média e desvio padrão próximo a 0 para todos esses casos
    # também não performou no mutual information, mi próximo a 0 para todos esses casos
    "bidirectional_cwr_packets",
    "src2dst_cwr_packets",
    "bidirectional_ece_packets",
    "dst2src_ece_packets",
    # não estavam nos pares de correlação, mas compartilham da média e desvio padrão próximo a 0.00, mi também irrisório
    "src2dst_ece_packets",
    "dst2src_cwr_packets",



    #("bidirectional_syn_packets", "src2dst_syn_packets"),
    # d2s não apareceu em correlação alta, mas indica caminho
    # pacotes > 0 aparecem em 75% dos dados
    # bidirectional corresponde a 30% da maior nota mi, manter por enquanto, mas testar o impacto em algum modelo de árvore
    "src2dst_syn_packets",
    "dst2src_syn_packets",
    #"bidirectional_syn_packets",

    #("bidirectional_fin_packets", "src2dst_fin_packets"),
    #("bidirectional_fin_packets", "dst2src_fin_packets"),
    # não houve alta correlação entre as variáveis de src e dst
    # removido somente por direção
    # mas assim como o ack, também está relacionado a coneões TCP
    # indica finalização de conexão TCP
    "src2dst_fin_packets",
    "dst2src_fin_packets",

    # nenhuma remoção em tcp_flags_pairs
    # nenhum remoção em volume_pairs

    # ps_pairs
    # remoção de direção

    # incluso na lista de alta correlação
    'src2dst_max_ps',
    'dst2src_stddev_ps',
    'dst2src_max_ps',
    'dst2src_mean_ps',
    'src2dst_stddev_ps',
    'src2dst_mean_ps',
    # não incluso na lista de alta correlação
    'src2dst_min_ps',
    'dst2src_min_ps',

    # soabrando apenas {'bidirectional_mean_ps', 'bidirectional_stddev_ps', 'bidirectional_max_ps'}

    # min_ps não apareceu nos pares altamente correlacionados.
    # verificar necessidade de manter min e max ps, pois podem indicar outliers extremos
    # talvez o desvio padrão seja o suficiente para generalizar

    # média e desvio possuem MI elevado
    # bidirectional_max_ps	MI 0.09 (elevado)
    # bidirectional_min_ps  MI 0.02 (baixo)

    # testar modelo de árvore
    # mean
    # mean + std
    # mean + max
    # mean + std + max
    # decisão de remoção adiada


    # testes
    # (mean, std, max) - as 3 informações são altamente correlacionadas
    # 'bidirectional_max_ps', -> irrelevante na seleção atual, mean e std já capturam esse comportamento
    #


    # ----------------------------------------------
    # como o mínimo apresentou MI baixo, foi removido
    "bidirectional_min_ps",

    # piat_pairs
    # PIAT = Packet Inter-Arrival Time -> tempo entre pacotes consecutivos
    # naturalmente um tamanho mínimo pode indicar flood de pactes

    # remoção de direção
    'dst2src_max_piat_ms',
    'src2dst_mean_piat_ms',
    'src2dst_max_piat_ms',

    # não incluso na lista de altamente correlacionados
    'src2dst_min_piat_ms',
    'src2dst_mean_piat_ms',
    'src2dst_stddev_piat_ms',
    'src2dst_max_piat_ms',
    'dst2src_min_piat_ms',
    'dst2src_mean_piat_ms',
    'dst2src_stddev_piat_ms',
    'dst2src_max_piat_ms',

    #validar com os mesmos testes para packet size


    # 'bidirectional_min_piat_ms', testado com/sem presença no modelo
    # a remoção não alterou praticamente nada, o que indica redundancia com outras features
    # isso também é percebido no MI
    'bidirectional_min_piat_ms',






    # outros de direção que sobraram e não apareceram em alta correlação
    # acumuladores de flags
    'src2dst_psh_packets',
    'src2dst_rst_packets',
    'dst2src_psh_packets',
    'dst2src_rst_packets',
    #bytes
    'src2dst_bytes',
    'dst2src_bytes',


    # outros que apresentaram MI muito baixo

    #bidirectional_urg_packets - mi : abaixo de 0.00

    "bidirectional_urg_packets",


}

# métricas src2dst/dst2src dependem da orientação adotada pelo NFStream,
# definida pelo primeiro pacote observado. Essa orientação pode variar entre
# capturas e fluxos interrompidos por timeout; são mantidas as métricas
# bidirecionais, que descrevem o comportamento agregado sem depender da direção.

DIRECTIONAL_PREFIXES = (
    "src2dst_",
    "dst2src_",
)





CATEGORICAL_COLUMNS = {
    "protocol",
    "src_port",
    "dst_port",
    "ip_version",
}



columns_to_ignore.update(const_features)
columns_to_ignore.update(sparse_tcp_features)
#comentado até verificar novamente esses atributos
#columns_to_ignore.update(identical_features)


def split_features(df):
    numeric_cols = df.select_dtypes(include='number').columns.tolist()

    valid_cols = [
        col for col in df.columns
        if col not in columns_to_ignore
           and not col.startswith(DIRECTIONAL_PREFIXES)
    ]

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

def get_columns_to_ignore():
    return columns_to_ignore



