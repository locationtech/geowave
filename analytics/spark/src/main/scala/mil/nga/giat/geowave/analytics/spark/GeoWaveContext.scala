package mil.nga.giat.geowave.analytics.spark

import mil.nga.giat.geowave.core.store.config.ConfigUtils
import mil.nga.giat.geowave.core.store.plugins.DataStorePluginOptions

class GeoWaveContext(
											val storeParameters: java.util.Map[String, String],
											val dataStoreName: String,
											val tableNameSpace: String) {
}

object GeoWaveContext {
	def apply(dataStoreOptions: DataStorePluginOptions,
						dataStoreName: String,
						tableNameSpace: String) = new GeoWaveContext(
		ConfigUtils.populateListFromOptions(dataStoreOptions.getFactoryOptions),
		dataStoreName,
		tableNameSpace);

}
