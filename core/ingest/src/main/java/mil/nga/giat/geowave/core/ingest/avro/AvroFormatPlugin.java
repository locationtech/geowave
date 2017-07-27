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

import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.index.IndexProvider;
import mil.nga.giat.geowave.core.ingest.local.LocalPluginBase;

/**
 * This is the main plugin interface for reading from a local file system, and
 * formatting intermediate data (for example, to HDFS or to Kafka for further
 * processing or ingest) from any file that is supported to Avro.
 * 
 * @param <I>
 *            The type for the input data
 * @param <O>
 *            The type that represents each data entry being ingested
 */
public interface AvroFormatPlugin<I, O> extends
		AvroPluginBase<I>,
		LocalPluginBase,
		IndexProvider
{

	/**
	 * An implementation of ingestion that ingests Avro Java objects into
	 * GeoWave
	 * 
	 * @return The implementation for ingestion from Avro
	 */
	public IngestPluginBase<I, O> getIngestWithAvroPlugin();

}
