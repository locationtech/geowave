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

import java.util.Collection;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.ingest.DataAdapterProvider;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This interface is used by the IngestFromHdfsPlugin to implement ingestion
 * with a mapper to aggregate key value pairs and a reducer to ingest data into
 * GeoWave. The implementation will be directly persisted to the job
 * configuration and called to generate key value pairs from intermediate data
 * in the mapper and to produce GeoWaveData to be written in the reducer.
 * 
 * @param <I>
 *            data type for intermediate data
 * @param K
 *            the type for the keys to be produced by the mapper from
 *            intermediate data, this should be the concrete type that is used
 *            because through reflection it will be given as the key class for
 *            map-reduce
 * @param V
 *            the type for the values to be produced by the mapper from
 *            intermediate data, this should be the concrete type that is used
 *            because through reflection it will be given as the value class for
 *            map-reduce
 * @param <O>
 *            data type that will be ingested into GeoWave
 */
public interface IngestWithReducer<I, K extends WritableComparable<?>, V extends Writable, O> extends
		DataAdapterProvider<O>,
		Persistable
{
	public CloseableIterator<KeyValueData<K, V>> toIntermediateMapReduceData(
			I input );

	public CloseableIterator<GeoWaveData<O>> toGeoWaveData(
			K key,
			Collection<ByteArrayId> primaryIndexIds,
			String globalVisibility,
			Iterable<V> values );
}
