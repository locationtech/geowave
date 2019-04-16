from py4j.java_gateway import JavaGateway, GatewayParameters, java_import

class GlobalConfigurations:
        """Sets up for gateway, and does imports"""

        def init(self):
                # Set-up Main Gateway Connection to JVM
                self.GATEWAY = JavaGateway(gateway_parameters=GatewayParameters(auto_field=True))

                ### Import Java Modules and Define Names here for easier access: ###

                # Geowave Core Store
                java_import(self.GATEWAY.jvm, "org.locationtech.geowave.core.store.api")
                self.MODULE__core_store = self.GATEWAY.jvm.org.locationtech.geowave.core.store.api

                # Geowave RocksDb Config
                java_import(self.GATEWAY.jvm, "org.locationtech.geowave.datastore.rocksdb.config")
                self.MODULE__rocksdb_config = self.GATEWAY.jvm.org.locationtech.geowave.datastore.rocksdb.config

                # Geowave Redis Config
                java_import(self.GATEWAY.jvm, "org.locationtech.geowave.datastore.redis.config")
                self.MODULE__redis_config = self.GATEWAY.jvm.org.locationtech.geowave.datastore.redis.config

                # Geowave Geotime Ingest
                java_import(self.GATEWAY.jvm, "org.locationtech.geowave.core.geotime.ingest")
                self.MODULE__geotime_ingest = self.GATEWAY.jvm.org.locationtech.geowave.core.geotime.ingest

                # Geotools Feature Simple
                java_import(config.GATEWAY.jvm, "org.geotools.feature.simple")
                self.MODULE__feature_simple = self.GATEWAY.jvm.org.geotools.feature.simple

                # Geowave Query Constraints
                java_import(self.GATEWAY.jvm, "org.locationtech.geowave.core.store.query.constraints")
                self.MODULE__query_constraints = self.GATEWAY.jvm.org.locationtech.geowave.core.store.query.constraints

                # Geowave Geotime Query API
                java_import(self.GATEWAY.jvm, "org.locationtech.geowave.core.geotime.store.query.api")
                self.MODULE__geotime_query = self.GATEWAY.jvm.org.locationtech.geowave.core.geotime.store.query.api


# Note - Module-wide Singleton!
global config
config = GlobalConfigurations()