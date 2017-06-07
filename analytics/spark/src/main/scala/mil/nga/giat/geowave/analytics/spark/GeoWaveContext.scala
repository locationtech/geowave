/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytics.spark

import mil.nga.giat.geowave.core.store.config.ConfigUtils
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions

class GeoWaveContext(
											val storeParameters: java.util.Map[String, String],
											val dataStoreName: String,
											val tableNameSpace: String) {
}

object GeoWaveContext {
	def apply(dataStoreOptions: DataStorePluginOptions,
						dataStoreName: String,
						tableNameSpace: String) = new GeoWaveContext(
		dataStoreOptions.getOptionsAsMap,
		dataStoreName,
		tableNameSpace);

}
