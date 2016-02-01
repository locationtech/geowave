package mil.nga.giat.geowave.analytics.spark

import mil.nga.giat.geowave.core.store.config.ConfigUtils
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions

class GeoWaveContext(
											val storeParameters: java.util.Map[String, String],
											val dataStoreName: String,
											val tableNameSpace: String) {
}

object GeoWaveContext {
	def apply(dataStoreOptions: DataStoreCommandLineOptions,
						dataStoreName: String,
						tableNameSpace: String) = new GeoWaveContext(
		ConfigUtils.valuesToStrings(
			dataStoreOptions.getConfigOptions(),
			dataStoreOptions.getFactory().getOptions()),
		dataStoreName,
		tableNameSpace);

}
