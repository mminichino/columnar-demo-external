##
##

import logging
from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions, ClusterOptions, TLSVerifyMode, AnalyticsOptions
from couchbase.cluster import Cluster

logger = logging.getLogger('columnardemo.columnar_driver')
logger.addHandler(logging.NullHandler())


class CBSession(object):

    def __init__(self, hostname: str, username: str, password: str, ssl=True, kv_timeout: int = 5, query_timeout: int = 60):
        self.cluster_node_count = None
        self._cluster = None
        self._bucket = None
        self._scope = None
        self._collection = None
        self._bucket_name = None
        self._scope_name = "_default"
        self._collection_name = "_default"
        self.ssl = ssl
        self.kv_timeout = kv_timeout
        self.query_timeout = query_timeout
        self.hostname = hostname
        self.username = username
        self.password = password

        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=query_timeout),
                                              kv_timeout=timedelta(seconds=kv_timeout),
                                              bootstrap_timeout=timedelta(seconds=kv_timeout * 2),
                                              resolve_timeout=timedelta(seconds=kv_timeout),
                                              connect_timeout=timedelta(seconds=kv_timeout),
                                              management_timeout=timedelta(seconds=kv_timeout * 2))

        if self.ssl:
            self.prefix = "https://"
            self.cb_prefix = "couchbases://"
            self.srv_prefix = "_couchbases._tcp."
            self.admin_port = 18091
            self.node_port = 19102
        else:
            self.prefix = "http://"
            self.cb_prefix = "couchbase://"
            self.srv_prefix = "_couchbase._tcp."
            self.admin_port = 8091
            self.node_port = 9102

        self.cluster_options = ClusterOptions(self.auth,
                                              timeout_options=self.timeouts,
                                              enable_tls=self.ssl,
                                              tls_verify=TLSVerifyMode.NO_VERIFY)

    @property
    def cb_connect_string(self):
        connect_string = self.cb_prefix + self.hostname
        logger.debug(f"Connect string: {connect_string}")
        return connect_string

    def session(self):
        self._cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
        self._cluster.wait_until_ready(timedelta(seconds=5))
        return self

    def connect_bucket(self, bucket_name: str):
        self._bucket = self._cluster.bucket(bucket_name)
        self._bucket_name = bucket_name
        return self

    def connect_scope(self, scope_name: str):
        self._scope = self._bucket.scope(scope_name)
        self._scope_name = scope_name
        return self

    def bucket_name(self, bucket_name: str):
        self._bucket_name = bucket_name
        return self

    def scope_name(self, scope_name: str):
        self._scope_name = scope_name
        return self

    def analytics_query(self, query: str):
        contents = []
        result = self._cluster.analytics_query(query, AnalyticsOptions(query_context=f"default:{self._bucket_name}.{self._scope_name}"))
        for item in result:
            contents.append(item)
        return contents
