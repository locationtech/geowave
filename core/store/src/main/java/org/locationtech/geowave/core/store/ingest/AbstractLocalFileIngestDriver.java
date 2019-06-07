/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FilenameUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This extends the local file driver to directly ingest data into GeoWave utilizing the
 * LocalFileIngestPlugin's that are discovered by the system.
 */
abstract public class AbstractLocalFileIngestDriver extends
    AbstractLocalFileDriver<LocalFileIngestPlugin<?>, LocalIngestRunData> {
  private static final int INGEST_BATCH_SIZE = 50000;
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLocalFileIngestDriver.class);
  protected ExecutorService ingestExecutor;

  public AbstractLocalFileIngestDriver() {
    super();
  }

  public AbstractLocalFileIngestDriver(final LocalInputCommandLineOptions inputOptions) {
    super(inputOptions);
  }

  public boolean runOperation(final String inputPath, final File configFile) {
    // first collect the local file ingest plugins
    final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<>();
    final List<DataTypeAdapter<?>> adapters = new ArrayList<>();
    for (final Entry<String, LocalFileIngestPlugin<?>> pluginEntry : getIngestPlugins().entrySet()) {

      if (!isSupported(pluginEntry.getKey(), pluginEntry.getValue())) {
        continue;
      }

      localFileIngestPlugins.put(pluginEntry.getKey(), pluginEntry.getValue());

      adapters.addAll(Arrays.asList(pluginEntry.getValue().getDataAdapters(getGlobalVisibility())));
    }

    final DataStore dataStore = getDataStore();
    try (LocalIngestRunData runData = new LocalIngestRunData(adapters, dataStore)) {

      startExecutor();

      processInput(inputPath, configFile, localFileIngestPlugins, runData);

      // We place this here and not just in finally because of the way
      // that try-with-resources works.
      // We want to wait for our ingesting threads to finish before we
      // kill our index writers, which
      // are cached in LocalIngestRunData. If we were don't, then the
      // index writers will be
      // closed before they are finished processing the file entries.
      shutdownExecutor();
    } catch (final IOException e) {
      LOGGER.error("Unexpected I/O exception when reading input files", e);
      return false;
    } finally {
      shutdownExecutor();
    }
    return true;
  }

  /**
   * Create a basic thread pool to ingest file data. We limit it to the amount of threads specified
   * on the command line.
   */
  public void startExecutor() {
    if (getNumThreads() > 1) {
      ingestExecutor = Executors.newFixedThreadPool(getNumThreads());
    }
  }

  /** This function will wait for executing tasks to complete for up to 10 seconds. */
  public void shutdownExecutor() {
    if (ingestExecutor != null) {
      try {
        ingestExecutor.shutdown();
        while (!ingestExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          LOGGER.debug("Waiting for ingest executor to terminate");
        }
      } catch (final InterruptedException e) {
        LOGGER.error("Failed to terminate executor service");
      } finally {
        ingestExecutor = null;
      }
    }
  }

  @Override
  public void processFile(
      final URL file,
      final String typeName,
      final LocalFileIngestPlugin<?> plugin,
      final LocalIngestRunData ingestRunData) throws IOException {

    LOGGER.info(
        String.format(
            "Beginning ingest for file: [%s]",
            // file.getName()));
            FilenameUtils.getName(file.getPath())));

    // This loads up the primary indexes that are specified on the command
    // line.
    // Usually spatial or spatial-temporal
    final Map<String, Index> specifiedPrimaryIndexes = getIndices();


    // This gets the list of required indexes from the Plugin.
    // If for some reason a GeoWaveData specifies an index that isn't
    // originally
    // in the specifiedPrimaryIndexes list, then this array is used to
    // determine
    // if the Plugin supports it. If it does, then we allow the creation of
    // the
    // index.
    final Map<String, Index> requiredIndexMap = new HashMap<>();
    final Index[] requiredIndices = plugin.getRequiredIndices();
    if ((requiredIndices != null) && (requiredIndices.length > 0)) {
      for (final Index requiredIndex : requiredIndices) {
        requiredIndexMap.put(requiredIndex.getName(), requiredIndex);
      }
    }

    if (getNumThreads() == 1) {
      processFileSingleThreaded(
          file,
          typeName,
          plugin,
          ingestRunData,
          specifiedPrimaryIndexes,
          requiredIndexMap,
          getGlobalVisibility());
    } else {
      processFileMultiThreaded(
          file,
          typeName,
          plugin,
          ingestRunData,
          specifiedPrimaryIndexes,
          requiredIndexMap);
    }

    LOGGER.info(String.format("Finished ingest for file: [%s]", file.getFile()));
  }

  public void processFileSingleThreaded(
      final URL file,
      final String typeName,
      final LocalFileIngestPlugin<?> plugin,
      final LocalIngestRunData ingestRunData,
      final Map<String, Index> specifiedPrimaryIndexes,
      final Map<String, Index> requiredIndexMap,
      final String globalVisibility) throws IOException {

    int count = 0;
    long dbWriteMs = 0L;
    final Map<String, Writer> indexWriters = new HashMap<>();
    // Read files until EOF from the command line.
    try (CloseableIterator<?> geowaveDataIt =
        plugin.toGeoWaveData(
            file,
            specifiedPrimaryIndexes.keySet().toArray(new String[0]),
            globalVisibility)) {

      while (geowaveDataIt.hasNext()) {
        final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
        try {
          final DataTypeAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
          if (adapter == null) {
            LOGGER.warn(
                String.format(
                    "Adapter not found for [%s] file [%s]",
                    geowaveData.getValue(),
                    FilenameUtils.getName(file.getPath())));
            continue;
          }

          // Ingest the data!
          dbWriteMs +=
              ingestData(
                  geowaveData,
                  adapter,
                  ingestRunData,
                  specifiedPrimaryIndexes,
                  requiredIndexMap,
                  indexWriters);

          count++;

        } catch (final Exception e) {
          throw new RuntimeException("Interrupted ingesting GeoWaveData", e);
        }
      }

      LOGGER.debug(
          String.format(
              "Finished ingest for file: [%s]; Ingested %d items in %d seconds",
              FilenameUtils.getName(file.getPath()),
              count,
              (int) dbWriteMs / 1000));

    } finally {
      // Clean up index writers
      for (final Entry<String, Writer> writerEntry : indexWriters.entrySet()) {
        try {
          ingestRunData.releaseIndexWriter(writerEntry.getKey(), writerEntry.getValue());
        } catch (final Exception e) {
          LOGGER.warn(
              String.format("Could not return index writer: [%s]", writerEntry.getKey()),
              e);
        }
      }
    }
  }

  private long ingestData(
      final GeoWaveData<?> geowaveData,
      final DataTypeAdapter adapter,
      final LocalIngestRunData runData,
      final Map<String, Index> specifiedPrimaryIndexes,
      final Map<String, Index> requiredIndexMap,
      final Map<String, Writer> indexWriters) throws Exception {

    try {
      final String adapterId = adapter.getTypeName();
      // Write the data to the data store.
      Writer writer = indexWriters.get(adapterId);

      if (writer == null) {
        final List<Index> indices = new ArrayList<>();
        for (final String indexName : geowaveData.getIndexNames()) {
          Index index = specifiedPrimaryIndexes.get(indexName);
          if (index == null) {
            index = requiredIndexMap.get(indexName);
            if (index == null) {
              LOGGER.warn(
                  String.format("Index '%s' not found for %s", indexName, geowaveData.getValue()));
              continue;
            }
          }
          indices.add(index);
        }
        runData.addAdapter(adapter);

        // If we have the index checked out already, use that.
        writer = runData.getIndexWriter(adapterId, indices);
        indexWriters.put(adapterId, writer);
      }

      // Time the DB write
      final long hack = System.currentTimeMillis();
      write(writer, geowaveData);
      final long durMs = System.currentTimeMillis() - hack;

      return durMs;
    } catch (final Exception e) {
      // This should really never happen, because we don't limit the
      // amount of items in the IndexWriter pool.
      LOGGER.error("Fatal error occured while trying write to an index writer.", e);
      throw new RuntimeException("Fatal error occured while trying write to an index writer.", e);
    }
  }

  protected void write(final Writer writer, final GeoWaveData<?> geowaveData) {
    writer.write(geowaveData.getValue());
  }

  public void processFileMultiThreaded(
      final URL file,
      final String typeName,
      final LocalFileIngestPlugin<?> plugin,
      final LocalIngestRunData ingestRunData,
      final Map<String, Index> specifiedPrimaryIndexes,
      final Map<String, Index> requiredIndexMap) throws IOException {

    // Create our queue. We will post GeoWaveData items to these queue until
    // there are no more items, at which point we will tell the workers to
    // complete. Ingest batch size is the total max number of items to read
    // from the file at a time for the worker threads to execute.
    final BlockingQueue<GeoWaveData<?>> queue = createBlockingQueue(INGEST_BATCH_SIZE);

    // Create our Jobs. We submit as many jobs as we have executors for.
    // These folks will read our blocking queue
    LOGGER.debug(
        String.format(
            "Creating [%d] threads to ingest file: [%s]",
            getNumThreads(),
            FilenameUtils.getName(file.getPath())));
    final List<IngestTask> ingestTasks = new ArrayList<>();
    try {
      for (int i = 0; i < getNumThreads(); i++) {
        final String id = String.format("%s-%d", FilenameUtils.getName(file.getPath()), i);
        final IngestTask task =
            new IngestTask(
                id,
                ingestRunData,
                specifiedPrimaryIndexes,
                requiredIndexMap,
                queue,
                this);
        ingestTasks.add(task);
        ingestExecutor.submit(task);
      }

      // Read files until EOF from the command line.
      try (CloseableIterator<?> geowaveDataIt =
          plugin.toGeoWaveData(
              file,
              specifiedPrimaryIndexes.keySet().toArray(new String[0]),
              getGlobalVisibility())) {

        while (geowaveDataIt.hasNext()) {
          final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
          try {
            while (!queue.offer(geowaveData, 100, TimeUnit.MILLISECONDS)) {
              // Determine if we have any workers left. The point
              // of this code is so we
              // aren't hanging after our workers exit (before the
              // file is done) due to
              // some un-handled exception.
              boolean workerAlive = false;
              for (final IngestTask task : ingestTasks) {
                if (!task.isFinished()) {
                  workerAlive = true;
                  break;
                }
              }

              // If the workers are still there, then just try to
              // offer again.
              // This will loop forever until there are no workers
              // left.
              if (workerAlive) {
                LOGGER.debug("Worker threads are overwhelmed, waiting 1 second");
              } else {
                final String message = "Datastore error, all workers have terminated! Aborting...";
                LOGGER.error(message);
                throw new RuntimeException(message);
              }
            }
          } catch (final InterruptedException e) {
            // I can't see how this will ever happen, except maybe
            // someone kills the process?
            throw new RuntimeException("Interrupted placing GeoWaveData on queue");
          }
        }
      }
    } finally {
      // Terminate our ingest tasks.
      for (final IngestTask task : ingestTasks) {
        task.terminate();
      }
    }
  }

  abstract protected int getNumThreads();

  abstract protected String getGlobalVisibility();

  abstract protected Map<String, LocalFileIngestPlugin<?>> getIngestPlugins();

  abstract protected DataStore getDataStore();

  abstract protected Map<String, Index> getIndices() throws IOException;

  abstract protected boolean isSupported(
      final String providerName,
      final DataAdapterProvider<?> adapterProvider);

  private static BlockingQueue<GeoWaveData<?>> createBlockingQueue(final int batchSize) {
    return new LinkedBlockingQueue<>(batchSize);
  }
}
