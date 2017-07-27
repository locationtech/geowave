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
package mil.nga.giat.geowave.core.ingest.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IngestTask is a thread which listens to items from a blocking queue, and
 * writes those items to IndexWriter objects obtained from LocalIngestRunData
 * (where they are constructed but also cached from the DataStore object). Read
 * items until isTerminated == true.
 */
public class IngestTask implements
		Runnable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestTask.class);
	private final String id;
	private final BlockingQueue<GeoWaveData<?>> readQueue;
	private final LocalIngestRunData runData;
	private final Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes;
	private final Map<ByteArrayId, PrimaryIndex> requiredIndexMap;
	private volatile boolean isTerminated = false;
	private volatile boolean isFinished = false;

	private Map<ByteArrayId, IndexWriter> indexWriters;
	private Map<ByteArrayId, AdapterToIndexMapping> adapterMappings;

	public IngestTask(
			String id,
			LocalIngestRunData runData,
			Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes,
			Map<ByteArrayId, PrimaryIndex> requiredIndexMap,
			BlockingQueue<GeoWaveData<?>> queue ) {
		this.id = id;
		this.runData = runData;
		this.specifiedPrimaryIndexes = specifiedPrimaryIndexes;
		this.requiredIndexMap = requiredIndexMap;
		this.readQueue = queue;

		this.indexWriters = new HashMap<ByteArrayId, IndexWriter>();
		this.adapterMappings = new HashMap<ByteArrayId, AdapterToIndexMapping>();
	}

	/**
	 * This function is called by the thread placing items on the blocking
	 * queue.
	 */
	public void terminate() {
		isTerminated = true;
	}

	/**
	 * An identifier, usually (filename)-(counter)
	 * 
	 * @return
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * Whether this worker has terminated.
	 * 
	 * @return
	 */
	public boolean isFinished() {
		return isFinished;
	}

	/**
	 * This function will continue to read from the BlockingQueue until
	 * isTerminated is true and the queue is empty.
	 */
	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	@Override
	public void run() {
		int count = 0;
		long dbWriteMs = 0L;

		try {
			LOGGER.debug(String.format(
					"Worker executing for plugin [%s]",
					this.getId()));

			while (true) {
				GeoWaveData<?> geowaveData = readQueue.poll(
						100,
						TimeUnit.MILLISECONDS);
				if (geowaveData == null) {
					if (isTerminated && readQueue.size() == 0) {
						// Done!
						break;
					}
					// Didn't receive an item. Make sure we haven't been
					// terminated.
					LOGGER.debug(String.format(
							"Worker waiting for item [%s]",
							this.getId()));

					continue;
				}

				final WritableDataAdapter adapter = runData.getDataAdapter(geowaveData);
				if (adapter == null) {
					LOGGER.warn(String.format(
							"Adapter not found for [%s] worker [%s]",
							geowaveData.getValue(),
							this.getId()));
					continue;
				}

				// Ingest the data!
				dbWriteMs += ingestData(
						geowaveData,
						adapter);

				count++;
			}
		}
		catch (Exception e) {
			// This should really never happen, because we don't limit the
			// amount of items in the IndexWriter pool.
			LOGGER.error(
					"Fatal error occured while trying to get an index writer.",
					e);
			throw new RuntimeException(
					"Fatal error occured while trying to get an index writer.",
					e);
		}
		finally {
			// Clean up index writers
			for (Entry<ByteArrayId, IndexWriter> writerEntry : indexWriters.entrySet()) {
				try {
					runData.releaseIndexWriter(
							adapterMappings.get(writerEntry.getKey()),
							writerEntry.getValue());
				}
				catch (Exception e) {
					LOGGER.warn(
							String.format(
									"Could not return index writer: [%s]",
									writerEntry.getKey()),
							e);
				}
			}

			LOGGER.debug(String.format(
					"Worker exited for plugin [%s]; Ingested %d items in %d seconds",
					this.getId(),
					count,
					(int) dbWriteMs / 1000));

			isFinished = true;
		}
	}

	private long ingestData(
			GeoWaveData<?> geowaveData,
			WritableDataAdapter adapter )
			throws Exception {
		AdapterToIndexMapping mapping = adapterMappings.get(adapter.getAdapterId());

		if (mapping == null) {
			List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>();
			for (final ByteArrayId indexId : geowaveData.getIndexIds()) {
				PrimaryIndex index = specifiedPrimaryIndexes.get(indexId);
				if (index == null) {
					index = requiredIndexMap.get(indexId);
					if (index == null) {
						LOGGER.warn(String.format(
								"Index '%s' not found for %s; worker [%s]",
								indexId.getString(),
								geowaveData.getValue(),
								this.getId()));
						continue;
					}
				}
				indices.add(index);
			}
			runData.addIndices(indices);
			runData.addAdapter(adapter);

			mapping = new AdapterToIndexMapping(
					adapter.getAdapterId(),
					indices.toArray(new PrimaryIndex[indices.size()]));
			adapterMappings.put(
					mapping.getAdapterId(),
					mapping);

			// If we have the index checked out already, use that.
			if (!indexWriters.containsKey(mapping.getAdapterId())) {
				indexWriters.put(
						mapping.getAdapterId(),
						runData.getIndexWriter(mapping));
			}
		}

		// Write the data to the data store.
		IndexWriter writer = indexWriters.get(mapping.getAdapterId());

		// Time the DB write
		long hack = System.currentTimeMillis();
		writer.write(geowaveData.getValue());
		long durMs = System.currentTimeMillis() - hack;

		return durMs;
	}
}
