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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;

/**
 * This extends the local file driver to directly ingest data into GeoWave
 * utilizing the LocalFileIngestPlugin's that are discovered by the system.
 */
public class LocalFileIngestDriver extends
		AbstractLocalFileDriver<LocalFileIngestPlugin<?>, LocalIngestRunData>
{
	public final static int INGEST_BATCH_SIZE = 500;
	private final static Logger LOGGER = LoggerFactory.getLogger(LocalFileIngestDriver.class);
	protected DataStorePluginOptions storeOptions;
	protected List<IndexPluginOptions> indexOptions;
	protected VisibilityOptions ingestOptions;
	protected Map<String, LocalFileIngestPlugin<?>> ingestPlugins;
	protected int threads;
	protected ExecutorService ingestExecutor;

	public LocalFileIngestDriver(
			DataStorePluginOptions storeOptions,
			List<IndexPluginOptions> indexOptions,
			Map<String, LocalFileIngestPlugin<?>> ingestPlugins,
			VisibilityOptions ingestOptions,
			LocalInputCommandLineOptions inputOptions,
			int threads ) {
		super(
				inputOptions);
		this.storeOptions = storeOptions;
		this.indexOptions = indexOptions;
		this.ingestOptions = ingestOptions;
		this.ingestPlugins = ingestPlugins;
		this.threads = threads;
	}

	public boolean runOperation(
			String inputPath,
			File configFile ) {
		// first collect the local file ingest plugins
		final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<String, LocalFileIngestPlugin<?>>();
		final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();
		for (Entry<String, LocalFileIngestPlugin<?>> pluginEntry : ingestPlugins.entrySet()) {

			if (!IngestUtils.checkIndexesAgainstProvider(
					pluginEntry.getKey(),
					pluginEntry.getValue(),
					indexOptions)) {
				continue;
			}

			localFileIngestPlugins.put(
					pluginEntry.getKey(),
					pluginEntry.getValue());

			adapters.addAll(Arrays.asList(pluginEntry.getValue().getDataAdapters(
					ingestOptions.getVisibility())));
		}

		DataStore dataStore = storeOptions.createDataStore();
		try (LocalIngestRunData runData = new LocalIngestRunData(
				adapters,
				dataStore)) {

			startExecutor();

			processInput(
					inputPath,
					configFile,
					localFileIngestPlugins,
					runData);

			// We place this here and not just in finally because of the way
			// that try-with-resources works.
			// We want to wait for our ingesting threads to finish before we
			// kill our index writers, which
			// are cached in LocalIngestRunData. If we were don't, then the
			// index writers will be
			// closed before they are finished processing the file entries.
			shutdownExecutor();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unexpected I/O exception when reading input files",
					e);
			return false;
		}
		finally {
			shutdownExecutor();
		}
		return true;
	}

	/**
	 * Create a basic thread pool to ingest file data. We limit it to the amount
	 * of threads specified on the command line.
	 */
	public void startExecutor() {
		ingestExecutor = Executors.newFixedThreadPool(threads);
	}

	/**
	 * This function will wait for executing tasks to complete for up to 10
	 * seconds.
	 */
	public void shutdownExecutor() {
		if (ingestExecutor != null) {
			try {
				ingestExecutor.shutdown();
				while (!ingestExecutor.awaitTermination(
						10,
						TimeUnit.SECONDS)) {
					LOGGER.debug("Waiting for ingest executor to terminate");
				}
			}
			catch (InterruptedException e) {
				LOGGER.error("Failed to terminate executor service");
			}
			finally {
				ingestExecutor = null;
			}
		}
	}

	@Override
	public void processFile(
			final URL file,
			final String typeName,
			final LocalFileIngestPlugin<?> plugin,
			final LocalIngestRunData ingestRunData )
			throws IOException {

		LOGGER.info(String.format(
				"Beginning ingest for file: [%s]",
				// file.getName()));
				FilenameUtils.getName(file.getPath())));

		// This loads up the primary indexes that are specified on the command
		// line.
		// Usually spatial or spatial-temporal
		final Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes = new HashMap<ByteArrayId, PrimaryIndex>();
		for (final IndexPluginOptions dimensionType : indexOptions) {
			final PrimaryIndex primaryIndex = dimensionType.createPrimaryIndex();
			if (primaryIndex == null) {
				LOGGER.error("Could not get index instance, getIndex() returned null;");
				throw new IOException(
						"Could not get index instance, getIndex() returned null");
			}
			specifiedPrimaryIndexes.put(
					primaryIndex.getId(),
					primaryIndex);
		}

		// This gets the list of required indexes from the Plugin.
		// If for some reason a GeoWaveData specifies an index that isn't
		// originally
		// in the specifiedPrimaryIndexes list, then this array is used to
		// determine
		// if the Plugin supports it. If it does, then we allow the creation of
		// the
		// index.
		final Map<ByteArrayId, PrimaryIndex> requiredIndexMap = new HashMap<ByteArrayId, PrimaryIndex>();
		final PrimaryIndex[] requiredIndices = plugin.getRequiredIndices();
		if ((requiredIndices != null) && (requiredIndices.length > 0)) {
			for (final PrimaryIndex requiredIndex : requiredIndices) {
				requiredIndexMap.put(
						requiredIndex.getId(),
						requiredIndex);
			}
		}

		if (threads == 1) {

			processFileSingleThreaded(
					file,
					typeName,
					plugin,
					ingestRunData,
					specifiedPrimaryIndexes,
					requiredIndexMap);

		}
		else {

			processFileMultiThreaded(
					file,
					typeName,
					plugin,
					ingestRunData,
					specifiedPrimaryIndexes,
					requiredIndexMap);
		}

		LOGGER.info(String.format(
				"Finished ingest for file: [%s]",
				file.getFile()));
	}

	public void processFileSingleThreaded(
			final URL file,
			final String typeName,
			final LocalFileIngestPlugin<?> plugin,
			final LocalIngestRunData ingestRunData,
			final Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes,
			final Map<ByteArrayId, PrimaryIndex> requiredIndexMap )
			throws IOException {

		int count = 0;
		long dbWriteMs = 0L;
		Map<ByteArrayId, IndexWriter> indexWriters = new HashMap<ByteArrayId, IndexWriter>();
		Map<ByteArrayId, AdapterToIndexMapping> adapterMappings = new HashMap<ByteArrayId, AdapterToIndexMapping>();

		// Read files until EOF from the command line.
		try (CloseableIterator<?> geowaveDataIt = plugin.toGeoWaveData(
				file,
				specifiedPrimaryIndexes.keySet(),
				ingestOptions.getVisibility())) {

			while (geowaveDataIt.hasNext()) {
				final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
				try {
					final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
					if (adapter == null) {
						LOGGER.warn(String.format(
								"Adapter not found for [%s] file [%s]",
								geowaveData.getValue(),
								FilenameUtils.getName(file.getPath())));
						continue;
					}

					// Ingest the data!
					dbWriteMs += ingestData(
							geowaveData,
							adapter,
							ingestRunData,
							specifiedPrimaryIndexes,
							requiredIndexMap,
							indexWriters,
							adapterMappings);

					count++;

				}
				catch (Exception e) {
					throw new RuntimeException(
							"Interrupted ingesting GeoWaveData",
							e);
				}
			}

			LOGGER.debug(String.format(
					"Finished ingest for file: [%s]; Ingested %d items in %d seconds",
					FilenameUtils.getName(file.getPath()),
					count,
					(int) dbWriteMs / 1000));

		}
		finally {
			// Clean up index writers
			for (Entry<ByteArrayId, IndexWriter> writerEntry : indexWriters.entrySet()) {
				try {
					ingestRunData.releaseIndexWriter(
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

		}
	}

	private long ingestData(
			GeoWaveData<?> geowaveData,
			WritableDataAdapter adapter,
			LocalIngestRunData runData,
			Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes,
			Map<ByteArrayId, PrimaryIndex> requiredIndexMap,
			Map<ByteArrayId, IndexWriter> indexWriters,
			Map<ByteArrayId, AdapterToIndexMapping> adapterMappings )
			throws Exception {

		try {

			AdapterToIndexMapping mapping = adapterMappings.get(adapter.getAdapterId());

			if (mapping == null) {
				List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>();
				for (final ByteArrayId indexId : geowaveData.getIndexIds()) {
					PrimaryIndex index = specifiedPrimaryIndexes.get(indexId);
					if (index == null) {
						index = requiredIndexMap.get(indexId);
						if (index == null) {
							LOGGER.warn(String.format(
									"Index '%s' not found for %s",
									indexId.getString(),
									geowaveData.getValue()));
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
		catch (Exception e) {
			// This should really never happen, because we don't limit the
			// amount of items in the IndexWriter pool.
			LOGGER.error(
					"Fatal error occured while trying write to an index writer.",
					e);
			throw new RuntimeException(
					"Fatal error occured while trying write to an index writer.",
					e);
		}

	}

	public void processFileMultiThreaded(
			final URL file,
			final String typeName,
			final LocalFileIngestPlugin<?> plugin,
			final LocalIngestRunData ingestRunData,
			final Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes,
			final Map<ByteArrayId, PrimaryIndex> requiredIndexMap )
			throws IOException {

		// Create our queue. We will post GeoWaveData items to these queue until
		// there are no more items, at which point we will tell the workers to
		// complete. Ingest batch size is the total max number of items to read
		// from the file at a time for the worker threads to execute.
		BlockingQueue<GeoWaveData<?>> queue = createBlockingQueue(INGEST_BATCH_SIZE);

		// Create our Jobs. We submit as many jobs as we have executors for.
		// These folks will read our blocking queue
		LOGGER.debug(String.format(
				"Creating [%d] threads to ingest file: [%s]",
				threads,
				FilenameUtils.getName(file.getPath())));
		List<IngestTask> ingestTasks = new ArrayList<IngestTask>();
		try {
			for (int i = 0; i < threads; i++) {
				String id = String.format(
						"%s-%d",
						FilenameUtils.getName(file.getPath()),
						i);
				IngestTask task = new IngestTask(
						id,
						ingestRunData,
						specifiedPrimaryIndexes,
						requiredIndexMap,
						queue);
				ingestTasks.add(task);
				ingestExecutor.submit(task);
			}

			// Read files until EOF from the command line.
			try (CloseableIterator<?> geowaveDataIt = plugin.toGeoWaveData(
					file,
					specifiedPrimaryIndexes.keySet(),
					ingestOptions.getVisibility())) {

				while (geowaveDataIt.hasNext()) {
					final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
					try {
						while (!queue.offer(
								geowaveData,
								100,
								TimeUnit.MILLISECONDS)) {
							// Determine if we have any workers left. The point
							// of this code is so we
							// aren't hanging after our workers exit (before the
							// file is done) due to
							// some un-handled exception.
							boolean workerAlive = false;
							for (IngestTask task : ingestTasks) {
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
							}
							else {
								String message = "Datastore error, all workers have terminated! Aborting...";
								LOGGER.error(message);
								throw new RuntimeException(
										message);
							}
						}
					}
					catch (InterruptedException e) {
						// I can't see how this will ever happen, except maybe
						// someone kills the process?
						throw new RuntimeException(
								"Interrupted placing GeoWaveData on queue");
					}
				}
			}
		}
		finally {
			// Terminate our ingest tasks.
			for (IngestTask task : ingestTasks) {
				task.terminate();
			}
		}

	}

	private static BlockingQueue<GeoWaveData<?>> createBlockingQueue(
			int batchSize ) {
		return new ArrayBlockingQueue<GeoWaveData<?>>(
				batchSize);
	}

}
