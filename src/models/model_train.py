""""

from sklearn import tree
X = [[0, 0], [1, 1]]
Y = [0, 1]
clf = tree.DecisionTreeClassifier()
clf = clf.fit(X, Y)

"""
import sys

from sklearn import tree
from sklearn.ensemble import RandomForestClassifier
from src.io.io_utils import load_parquet
from src.configs.paths import FINAL_DATA_DIR

from sklearn.model_selection import train_test_split

from src.utils.features import split_features

from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

mi_features = ['bidirectional_bytes',
 'bidirectional_mean_ps',
 'bidirectional_max_ps',
 'bidirectional_stddev_ps',
 'dst2src_mean_ps',
 'dst2src_bytes',
 'dst2src_max_ps',
 'dst2src_stddev_ps',
 'src2dst_max_ps',
 'src2dst_mean_ps',
 'src2dst_bytes',
 'src2dst_stddev_ps',
 'src2dst_max_piat_ms',
 'bidirectional_max_piat_ms',
 'dst2src_max_piat_ms',
 'bidirectional_mean_piat_ms',
 'bidirectional_stddev_piat_ms',
 'bidirectional_ack_packets',
 'src2dst_stddev_piat_ms',
 'dst2src_stddev_piat_ms',
 'src2dst_mean_piat_ms',
 'dst2src_mean_piat_ms',
 'dst2src_duration_ms',
 'src2dst_duration_ms',
 'src2dst_rst_packets',
 'bidirectional_packets',
 'dst2src_ack_packets',
 'dst2src_packets',
 'bidirectional_duration_ms',
 'bidirectional_rst_packets',
 'bidirectional_psh_packets',
 'dst2src_psh_packets',
 'src2dst_psh_packets',
 'src2dst_packets',
 'src2dst_ack_packets',
 'bidirectional_fin_packets',
 'bidirectional_syn_packets',
 'dst2src_syn_packets',
 'dst2src_fin_packets',
 'dst2src_min_ps',
 'src2dst_syn_packets',
 'src2dst_min_ps',
 'bidirectional_min_ps',
 'bidirectional_min_piat_ms',
 'dst2src_rst_packets',
 'src2dst_min_piat_ms',
 'src2dst_fin_packets',
 'dst2src_ece_packets',
 'bidirectional_ece_packets',
 'dst2src_min_piat_ms',
 'bidirectional_cwr_packets',
 'dst2src_cwr_packets',
 'bidirectional_urg_packets',
 'src2dst_ece_packets',
 'src2dst_cwr_packets',
 'src2dst_urg_packets',
 'dst2src_urg_packets']

def decision_tree_classifier(dataset_name):
    df = load_parquet(FINAL_DATA_DIR / "single"/f"{dataset_name}.parquet")
    df = df.sample(frac=0.2, random_state=1)

    # evitar o waning de poucos elementos por classe
    counts = df["label"].value_counts()
    valid_classes = counts[counts >= 100].index
    df = df[df["label"].isin(valid_classes)]



    numerical, categorical = split_features(df)

    #x = df[numerical]
    x = df[mi_features[:10]]
    y = df["label"]

    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=1, stratify=y)

    #clf = tree.DecisionTreeClassifier(class_weight="balanced")
    clf = RandomForestClassifier(n_estimators=100, random_state=1, n_jobs=-1, class_weight="balanced")
    clf = clf.fit(x_train, y_train)

    y_pred = clf.predict(x_test)

    print(classification_report(y_test, y_pred))


if __name__ == "__main__":
    decision_tree_classifier("cicids2017")