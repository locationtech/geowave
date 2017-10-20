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
package mil.nga.giat.geowave.core.ingest.avro;

import java.io.File;
import java.net.URL;

/**
 * All plugins based off of staged intermediate data (either reading or writing)
 * must implement this interface. For handling intermediate data, the GeoWave
 * ingestion framework has standardized on Avro for java object serialization
 * and an Avro schema must be provided for handling any intermediate data.
 */
public interface AvroPluginBase<T> extends
		AvroSchemaProvider
{
	/**
	 * Converts the supported file into an Avro encoded Java object.
	 * 
	 * @param file
	 *            The file to convert to Avro
	 * @return The Avro encoded Java object
	 */
	public T[] toAvroObjects(
			URL file );

}
