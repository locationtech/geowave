/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.ingest.avro.AbstractStageWholeFileToAvro;
import org.locationtech.geowave.core.ingest.avro.WholeFile;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be sub-classed as a general-purpose recipe for parallelizing
 * ingestion of files either locally or by directly staging the binary of the
 * file to HDFS and then ingesting it within the map phase of a map-reduce job.
 */
abstract public class AbstractLocalIngestWithMapper<T> extends
		AbstractStageWholeFileToAvro implements
		LocalFileIngestPlugin<T>,
		IngestFromHdfsPlugin<WholeFile, T>,
		Persistable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractLocalIngestWithMapper.class);

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<WholeFile, T> ingestWithMapper() {
		return new InternalIngestWithMapper<>(
				this);
	}

	@Override
	public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
			final URL input,
			final String[] indexNames,
			final String globalVisibility ) {
		try (final InputStream inputStream = input.openStream()) {
			return toGeoWaveDataInternal(
					inputStream,
					indexNames,
					globalVisibility);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Cannot open file, unable to ingest",
					e);
		}
		return new CloseableIterator.Wrapper(
				Collections.emptyIterator());
	}

	abstract protected CloseableIterator<GeoWaveData<T>> toGeoWaveDataInternal(
			final InputStream file,
			final String[] indexNames,
			final String globalVisibility );

	@Override
	public IngestWithReducer<WholeFile, ?, ?, T> ingestWithReducer() {
		return null;
	}

	protected static class InternalIngestWithMapper<T> implements
			IngestWithMapper<WholeFile, T>
	{
		private AbstractLocalIngestWithMapper parentPlugin;

		public InternalIngestWithMapper() {}

		public InternalIngestWithMapper(
				final AbstractLocalIngestWithMapper parentPlugin ) {
			this.parentPlugin = parentPlugin;
		}

		@Override
		public DataTypeAdapter<T>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
				final WholeFile input,
				final String[] indexNames,
				final String globalVisibility ) {
			final InputStream inputStream = new ByteBufferBackedInputStream(
					input.getOriginalFile());
			return parentPlugin.toGeoWaveDataInternal(
					inputStream,
					indexNames,
					globalVisibility);
		}

		@Override
		public byte[] toBinary() {
			return PersistenceUtils.toClassId(parentPlugin);
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			parentPlugin = (AbstractLocalIngestWithMapper) PersistenceUtils.fromClassId(bytes);
		}

		@Override
		public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
			return parentPlugin.getSupportedIndexableTypes();
		}
	}

}
