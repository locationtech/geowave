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
package mil.nga.giat.geowave.core.ingest.spi;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

/**
 * This interface can be injected and automatically discovered using SPI to
 * provide a new ingest format to the GeoWave ingestion framework. It is not
 * required that a new ingest format implement all of the plugins. However, each
 * plugin directly corresponds to a user selected operation and only the plugins
 * that are supported will result in usable operations.
 * 
 * @param <I>
 *            The type for intermediate data
 * @param <O>
 *            The type for the resulting data that is ingested into GeoWave
 */
public interface IngestFormatPluginProviderSpi<I, O>
{

	/**
	 * This plugin will be used by the ingestion framework to read data from
	 * HDFS in the form of the intermediate data format, and translate the
	 * intermediate data into the data entries that will be written in GeoWave.
	 * 
	 * @return The plugin for ingesting data from HDFS
	 * @throws UnsupportedOperationException
	 *             If ingesting intermediate data from HDFS is not supported
	 */
	public IngestFromHdfsPlugin<I, O> createIngestFromHdfsPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException;

	/**
	 * This plugin will be used by the ingestion framework to read data from a
	 * local file system, and translate supported files into the data entries
	 * that will be written directly in GeoWave.
	 * 
	 * @return The plugin for ingesting data from a local file system directly
	 *         into GeoWave
	 * @throws UnsupportedOperationException
	 *             If ingesting data directly from a local file system is not
	 *             supported
	 */
	public LocalFileIngestPlugin<O> createLocalFileIngestPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException;

	/**
	 * This will represent the name for the format that is registered with the
	 * ingest framework and presented as a data format option via the
	 * commandline. For consistency, this name is preferably lower-case and
	 * without spaces, and should uniquely identify the data format as much as
	 * possible.
	 * 
	 * @return The name that will be associated with this format
	 */
	public String getIngestFormatName();

	/**
	 * This is a means for a plugin to provide custom command-line options. If
	 * this is null, there will be no custom options added.
	 * 
	 * 
	 * @return The ingest format's option provider or null for no custom options
	 */
	public IngestFormatOptionProvider createOptionsInstances();

	/**
	 * This is a user-friendly full description of the data format that this
	 * plugin provider supports. It will be presented to the command-line user
	 * as help when the registered data formats are listed.
	 * 
	 * @return The user-friendly full description for this data format
	 */
	public String getIngestFormatDescription();

	/**
	 * This plugin will be used by the ingestion framework to stage intermediate
	 * data from a local filesystem (for example to HDFS for map reduce ingest
	 * or to kafka for kafka ingest).
	 * 
	 * @return The plugin for staging to avro if it is supported
	 * @throws UnsupportedOperationException
	 *             If staging data is not supported (generally this implies that
	 *             ingesting using map-reduce or kafka will not be supported)
	 */
	public AvroFormatPlugin<I, O> createAvroFormatPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException;
}
