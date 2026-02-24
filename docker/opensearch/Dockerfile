# 用途：建立含 IK 中文分詞插件的 OpenSearch 映像，供本機/EC2 可重現部署使用。
# 說明：固定使用 OpenSearch 2.18.0 與對應 IK plugin 版本，避免容器重建後插件遺失。
FROM opensearchproject/opensearch:2.18.0

RUN /usr/share/opensearch/bin/opensearch-plugin install --batch \
    https://get.infini.cloud/opensearch/analysis-ik/2.18.0
