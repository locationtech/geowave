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
package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.avro.AbstractStageWholeFileToAvro;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

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
		return new InternalIngestWithMapper<T>(
				this);
	}

	@Override
	public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
			final URL input,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		try (final InputStream inputStream = input.openStream()) {
			return toGeoWaveDataInternal(
					inputStream,
					primaryIndexIds,
					globalVisibility);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Cannot open file, unable to ingest",
					e);
		}
		return new CloseableIterator.Wrapper(
				Iterators.emptyIterator());
	}

	abstract protected CloseableIterator<GeoWaveData<T>> toGeoWaveDataInternal(
			final InputStream file,
			final Collection<ByteArrayId> primaryIndexIds,
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
		public WritableDataAdapter<T>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
				final WholeFile input,
				final Collection<ByteArrayId> primaryIndexIds,
				final String globalVisibility ) {
			final InputStream inputStream = new ByteBufferBackedInputStream(
					input.getOriginalFile());
			return parentPlugin.toGeoWaveDataInternal(
					inputStream,
					primaryIndexIds,
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
