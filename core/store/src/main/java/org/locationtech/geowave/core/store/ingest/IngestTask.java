/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IngestTask is a thread which listens to items from a blocking queue, and writes those items to
 * IndexWriter objects obtained from LocalIngestRunData (where they are constructed but also cached
 * from the DataStore object). Read items until isTerminated == true.
 */
public class IngestTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestTask.class);
  private final String id;
  private final BlockingQueue<GeoWaveData<?>> readQueue;
  private final LocalIngestRunData runData;
  private final Map<String, Index> specifiedPrimaryIndexes;
  private final Map<String, Index> requiredIndexMap;
  private volatile boolean isTerminated = false;
  private volatile boolean isFinished = false;

  private final Map<String, Writer> indexWriters;
  private final Map<String, AdapterToIndexMapping> adapterMappings;
  private final AbstractLocalFileIngestDriver localFileIngestDriver;

  public IngestTask(
      final String id,
      final LocalIngestRunData runData,
      final Map<String, Index> specifiedPrimaryIndexes,
      final Map<String, Index> requiredIndexMap,
      final BlockingQueue<GeoWaveData<?>> queue,
      final AbstractLocalFileIngestDriver localFileIngestDriver) {
    this.id = id;
    this.runData = runData;
    this.specifiedPrimaryIndexes = specifiedPrimaryIndexes;
    this.requiredIndexMap = requiredIndexMap;
    this.localFileIngestDriver = localFileIngestDriver;
    readQueue = queue;

    indexWriters = new HashMap<>();
    adapterMappings = new HashMap<>();
  }

  /** This function is called by the thread placing items on the blocking queue. */
  public void terminate() {
    isTerminated = true;
  }

  /**
   * An identifier, usually (filename)-(counter)
   *
   * @return the ID of the task
   */
  public String getId() {
    return id;
  }

  /**
   * Whether this worker has terminated.
   *
   * @return {@code true} if the task is finished
   */
  public boolean isFinished() {
    return isFinished;
  }

  /**
   * This function will continue to read from the BlockingQueue until isTerminated is true and the
   * queue is empty.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void run() {
    int count = 0;
    long dbWriteMs = 0L;

    try {
      LOGGER.debug(String.format("Worker executing for plugin [%s]", getId()));

      while (true) {
        final GeoWaveData<?> geowaveData = readQueue.poll(100, TimeUnit.MILLISECONDS);
        if (geowaveData == null) {
          if (isTerminated && (readQueue.size() == 0)) {
            // Done!
            break;
          }
          // Didn't receive an item. Make sure we haven't been
          // terminated.
          LOGGER.debug(String.format("Worker waiting for item [%s]", getId()));

          continue;
        }

        final DataTypeAdapter adapter = runData.getDataAdapter(geowaveData);
        if (adapter == null) {
          LOGGER.warn(
              String.format(
                  "Adapter not found for [%s] worker [%s]",
                  geowaveData.getValue(),
                  getId()));
          continue;
        }

        // Ingest the data!
        dbWriteMs += ingestData(geowaveData, adapter);

        count++;
      }
    } catch (final Exception e) {
      // This should really never happen, because we don't limit the
      // amount of items in the IndexWriter pool.
      LOGGER.error("Fatal error occured while trying to get an index writer.", e);
      throw new RuntimeException("Fatal error occured while trying to get an index writer.", e);
    } finally {
      // Clean up index writers
      for (final Entry<String, Writer> writerEntry : indexWriters.entrySet()) {
        try {
          runData.releaseIndexWriter(writerEntry.getKey(), writerEntry.getValue());
        } catch (final Exception e) {
          LOGGER.warn(
              String.format("Could not return index writer: [%s]", writerEntry.getKey()),
              e);
        }
      }

      LOGGER.debug(
          String.format(
              "Worker exited for plugin [%s]; Ingested %d items in %d seconds",
              getId(),
              count,
              (int) dbWriteMs / 1000));

      isFinished = true;
    }
  }

  private long ingestData(final GeoWaveData<?> geowaveData, final DataTypeAdapter adapter)
      throws Exception {

    final String typeName = adapter.getTypeName();
    // Write the data to the data store.
    Writer writer = indexWriters.get(typeName);

    if (writer == null) {
      final List<Index> indices = new ArrayList<>();
      for (final String indexName : geowaveData.getIndexNames()) {
        Index index = specifiedPrimaryIndexes.get(indexName);
        if (index == null) {
          index = requiredIndexMap.get(indexName);
          if (index == null) {
            LOGGER.warn(
                String.format(
                    "Index '%s' not found for %s; worker [%s]",
                    indexName,
                    geowaveData.getValue(),
                    getId()));
            continue;
          }
        }
        indices.add(index);
      }
      runData.addAdapter(adapter);

      // If we have the index checked out already, use that.
      writer = runData.getIndexWriter(typeName, indices);
      indexWriters.put(typeName, writer);
    }

    // Time the DB write
    final long hack = System.currentTimeMillis();
    localFileIngestDriver.write(writer, geowaveData);
    final long durMs = System.currentTimeMillis() - hack;

    return durMs;
  }
}
