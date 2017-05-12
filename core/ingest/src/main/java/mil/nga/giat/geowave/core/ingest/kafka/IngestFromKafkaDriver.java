package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.GenericAvroSerializer;
import mil.nga.giat.geowave.core.ingest.index.IndexProvider;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;

/**
 * This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 * 
 */
public class IngestFromKafkaDriver
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestFromKafkaDriver.class);

	private final DataStorePluginOptions storeOptions;
	private final List<IndexPluginOptions> indexOptions;
	private final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins;
	private final KafkaConsumerCommandLineOptions kafkaOptions;
	private final VisibilityOptions ingestOptions;
	private final List<Future<?>> futures = new ArrayList<Future<?>>();

	public IngestFromKafkaDriver(
			DataStorePluginOptions storeOptions,
			List<IndexPluginOptions> indexOptions,
			Map<String, AvroFormatPlugin<?, ?>> ingestPlugins,
			KafkaConsumerCommandLineOptions kafkaOptions,
			VisibilityOptions ingestOptions ) {
		this.storeOptions = storeOptions;
		this.indexOptions = indexOptions;
		this.ingestPlugins = ingestPlugins;
		this.kafkaOptions = kafkaOptions;
		this.ingestOptions = ingestOptions;
	}

	public boolean runOperation() {

		final DataStore dataStore = storeOptions.createDataStore();

		final List<String> queue = new ArrayList<String>();
		addPluginsToQueue(
				ingestPlugins,
				queue);

		configureAndLaunchPlugins(
				dataStore,
				ingestPlugins,
				queue);

		int counter = 0;
		while (queue.size() > 0) {
			if (counter > 30) {
				for (final String pluginFormatName : queue) {
					LOGGER.error("Unable to start up Kafka consumer for plugin [" + pluginFormatName + "]");
				}
				break;
			}
			try {
				Thread.sleep(1000);
			}
			catch (final InterruptedException e) {
				LOGGER.error(
						"Thread interrupted",
						e);
			}
			counter++;
		}

		if (queue.size() == 0) {
			LOGGER.info("All format plugins are now listening on Kafka topics");
		}
		else {
			LOGGER.warn("Unable to setup Kafka consumers for the following format plugins:");
			for (final String formatPluginName : queue) {
				LOGGER.warn("\t[" + formatPluginName + "]");
			}
			return false;
		}
		return true;
	}

	private void addPluginsToQueue(
			final Map<String, AvroFormatPlugin<?, ?>> pluginProviders,
			final List<String> queue ) {
		queue.addAll(pluginProviders.keySet());
	}

	private void configureAndLaunchPlugins(
			final DataStore dataStore,
			final Map<String, AvroFormatPlugin<?, ?>> pluginProviders,
			final List<String> queue ) {
		try {
			for (Entry<String, AvroFormatPlugin<?, ?>> pluginProvider : pluginProviders.entrySet()) {
				final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();

				AvroFormatPlugin<?, ?> avroFormatPlugin = null;
				try {
					avroFormatPlugin = pluginProvider.getValue();

					final IngestPluginBase<?, ?> ingestWithAvroPlugin = avroFormatPlugin.getIngestWithAvroPlugin();
					final WritableDataAdapter<?>[] dataAdapters = ingestWithAvroPlugin.getDataAdapters(ingestOptions
							.getVisibility());
					adapters.addAll(Arrays.asList(dataAdapters));
					final KafkaIngestRunData runData = new KafkaIngestRunData(
							adapters,
							dataStore);

					futures.add(launchTopicConsumer(
							pluginProvider.getKey(),
							avroFormatPlugin,
							runData,
							queue));
				}
				catch (final UnsupportedOperationException e) {
					LOGGER.warn(
							"Plugin provider '" + pluginProvider.getKey() + "' does not support ingest from Kafka",
							e);
					continue;
				}
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error in accessing Kafka stream",
					e);
		}
	}

	private ConsumerConnector buildKafkaConsumer() {

		Properties kafkaProperties = kafkaOptions.getProperties();

		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				kafkaProperties));

		return consumer;
	}

	private Future<?> launchTopicConsumer(
			final String formatPluginName,
			final AvroFormatPlugin<?, ?> avroFormatPlugin,
			final KafkaIngestRunData ingestRunData,
			final List<String> queue )
			throws IllegalArgumentException {
		final ExecutorService executorService = Executors.newFixedThreadPool(queue.size());
		return executorService.submit(new Runnable() {

			@Override
			public void run() {
				try {
					consumeFromTopic(
							formatPluginName,
							avroFormatPlugin,
							ingestRunData,
							queue);
				}
				catch (final Exception e) {
					LOGGER.error(
							"Error consuming from Kafka topic [" + formatPluginName + "]",
							e);
				}
			}
		});
	}

	public <T> void consumeFromTopic(
			final String formatPluginName,
			final AvroFormatPlugin<T, ?> avroFormatPlugin,
			final KafkaIngestRunData ingestRunData,
			final List<String> queue ) {

		final ConsumerConnector consumer = buildKafkaConsumer();
		if (consumer == null) {
			throw new RuntimeException(
					"Kafka consumer connector is null, unable to create message streams");
		}
		try {
			LOGGER.debug("Kafka consumer setup for format [" + formatPluginName + "] against topic ["
					+ formatPluginName + "]");
			final Map<String, Integer> topicCount = new HashMap<>();
			topicCount.put(
					formatPluginName,
					1);

			final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer
					.createMessageStreams(topicCount);
			final List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(formatPluginName);

			queue.remove(formatPluginName);
			consumeMessages(
					formatPluginName,
					avroFormatPlugin,
					ingestRunData,
					streams.get(0));
		}
		finally {
			consumer.shutdown();
		}
	}

	protected <T> void consumeMessages(
			final String formatPluginName,
			final AvroFormatPlugin<T, ?> avroFormatPlugin,
			final KafkaIngestRunData ingestRunData,
			final KafkaStream<byte[], byte[]> stream ) {
		int currentBatchId = 0;
		final int batchSize = kafkaOptions.getBatchSize();
		try {
			final ConsumerIterator<byte[], byte[]> messageIterator = stream.iterator();
			while (messageIterator.hasNext()) {
				final byte[] msg = messageIterator.next().message();
				LOGGER.info("[" + formatPluginName + "] message received");
				final T dataRecord = GenericAvroSerializer.deserialize(
						msg,
						avroFormatPlugin.getAvroSchema());

				if (dataRecord != null) {
					try {
						processMessage(
								dataRecord,
								ingestRunData,
								avroFormatPlugin);
						if (++currentBatchId > batchSize) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug(String.format(
										"Flushing %d items",
										currentBatchId));
							}
							ingestRunData.flush();
							currentBatchId = 0;
						}
					}
					catch (final Exception e) {
						LOGGER.error(
								"Error processing message: " + e.getMessage(),
								e);
					}
				}
			}
		}
		catch (final ConsumerTimeoutException te) {
			// Flush any outstanding items
			if (currentBatchId > 0) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format(
							"Flushing %d items",
							currentBatchId));
				}
				ingestRunData.flush();
				currentBatchId = 0;
			}
			if (kafkaOptions.isFlushAndReconnect()) {
				LOGGER.info(
						"Consumer timed out from Kafka topic [" + formatPluginName + "]... Reconnecting...",
						te);
				consumeMessages(
						formatPluginName,
						avroFormatPlugin,
						ingestRunData,
						stream);
			}
			else {
				LOGGER.info(
						"Consumer timed out from Kafka topic [" + formatPluginName + "]... ",
						te);
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Consuming from Kafka topic [" + formatPluginName + "] was interrupted... ",
					e);
		}

	}

	synchronized protected <T> void processMessage(
			final T dataRecord,
			final KafkaIngestRunData ingestRunData,
			final AvroFormatPlugin<T, ?> plugin )
			throws IOException {

		IngestPluginBase<T, ?> ingestPlugin = plugin.getIngestWithAvroPlugin();
		IndexProvider indexProvider = plugin;

		final Map<ByteArrayId, IndexWriter> writerMap = new HashMap<ByteArrayId, IndexWriter>();
		final Map<ByteArrayId, PrimaryIndex> indexMap = new HashMap<ByteArrayId, PrimaryIndex>();

		for (IndexPluginOptions indexOption : indexOptions) {
			final PrimaryIndex primaryIndex = indexOption.createPrimaryIndex();
			if (primaryIndex == null) {
				LOGGER.error("Could not get index instance, getIndex() returned null;");
				throw new IOException(
						"Could not get index instance, getIndex() returned null");
			}
			indexMap.put(
					primaryIndex.getId(),
					primaryIndex);
		}

		final PrimaryIndex[] requiredIndices = indexProvider.getRequiredIndices();
		if ((requiredIndices != null) && (requiredIndices.length > 0)) {
			for (final PrimaryIndex requiredIndex : requiredIndices) {
				indexMap.put(
						requiredIndex.getId(),
						requiredIndex);
			}
		}

		try (CloseableIterator<?> geowaveDataIt = ingestPlugin.toGeoWaveData(
				dataRecord,
				indexMap.keySet(),
				ingestOptions.getVisibility())) {
			while (geowaveDataIt.hasNext()) {
				final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
				final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
				if (adapter == null) {
					LOGGER.warn("Adapter not found for " + geowaveData.getValue());
					continue;
				}
				IndexWriter indexWriter = writerMap.get(adapter.getAdapterId());
				if (indexWriter == null) {
					List<PrimaryIndex> indexList = new ArrayList<PrimaryIndex>();
					for (final ByteArrayId indexId : geowaveData.getIndexIds()) {
						final PrimaryIndex index = indexMap.get(indexId);
						if (index == null) {
							LOGGER.warn("Index '" + indexId.getString() + "' not found for " + geowaveData.getValue());
							continue;
						}
						indexList.add(index);
					}
					indexWriter = ingestRunData.getIndexWriter(
							adapter,
							indexList.toArray(new PrimaryIndex[indexList.size()]));
					writerMap.put(
							adapter.getAdapterId(),
							indexWriter);
				}

				indexWriter.write(geowaveData.getValue());

			}
		}
	}

	public List<Future<?>> getFutures() {
		return futures;
	}

	/**
	 * Only true if all futures are complete.
	 * 
	 * @return
	 */
	public boolean isComplete() {
		for (Future<?> future : futures) {
			if (!future.isDone()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Wait for all kafka topics to complete, then return the result objects.
	 * 
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public List<Object> waitFutures()
			throws InterruptedException,
			ExecutionException {
		List<Object> results = new ArrayList<Object>();
		for (Future<?> future : futures) {
			results.add(future.get());
		}
		return results;
	}
}
