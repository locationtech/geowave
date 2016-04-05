package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.AbstractIngestCommandLineDriver;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.GenericAvroSerializer;
import mil.nga.giat.geowave.core.ingest.index.IndexProvider;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 * 
 */
public class IngestFromKafkaDriver extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(IngestFromKafkaDriver.class);

	private KafkaConsumerCommandLineOptions kafkaOptions;
	private DataStoreCommandLineOptions dataStoreOptions;
	private IngestCommandLineOptions ingestOptions;

	public IngestFromKafkaDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected boolean runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
		final DataStore dataStore = dataStoreOptions.createStore();

		final List<String> queue = new ArrayList<String>();
		addPluginsToQueue(
				pluginProviders,
				queue);

		configureAndLaunchPlugins(
				dataStore,
				pluginProviders,
				queue,
				args);

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
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders,
			final List<String> queue ) {
		for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			queue.add(pluginProvider.getIngestFormatName());
		}
	}

	private void configureAndLaunchPlugins(
			final DataStore dataStore,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders,
			final List<String> queue,
			final String[] args ) {
		try {
			for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
				final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();

				AvroFormatPlugin<?, ?> avroFormatPlugin = null;
				try {
					avroFormatPlugin = pluginProvider.getAvroFormatPlugin();
					if (avroFormatPlugin == null) {
						LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestFormatName() + "' does not support ingest from Kafka");
						continue;
					}

					final IngestPluginBase<?, ?> ingestWithAvroPlugin = avroFormatPlugin.getIngestWithAvroPlugin();
					final WritableDataAdapter<?>[] dataAdapters = ingestWithAvroPlugin.getDataAdapters(ingestOptions.getVisibility());
					adapters.addAll(Arrays.asList(dataAdapters));
					final KafkaIngestRunData runData = new KafkaIngestRunData(
							adapters,
							dataStore,
							args);

					launchTopicConsumer(
							pluginProvider.getIngestFormatName(),
							avroFormatPlugin,
							runData,
							queue);
				}
				catch (final UnsupportedOperationException e) {
					LOGGER.warn(
							"Plugin provider '" + pluginProvider.getIngestFormatName() + "' does not support ingest from Kafka",
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

		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				kafkaOptions.getProperties()));

		return consumer;
	}

	private void launchTopicConsumer(
			final String formatPluginName,
			final AvroFormatPlugin<?, ?> avroFormatPlugin,
			final KafkaIngestRunData ingestRunData,
			final List<String> queue )
			throws Exception {
		final ExecutorService executorService = Executors.newFixedThreadPool(queue.size());
		executorService.execute(new Runnable() {

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
			final List<String> queue )
			throws Exception {

		final ConsumerConnector consumer = buildKafkaConsumer();
		if (consumer == null) {
			throw new Exception(
					"Kafka consumer connector is null, unable to create message streams");
		}
		try {
			LOGGER.debug("Kafka consumer setup for format [" + formatPluginName + "] against topic [" + formatPluginName + "]");
			final Map<String, Integer> topicCount = new HashMap<>();
			topicCount.put(
					formatPluginName,
					1);

			final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
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
			if (kafkaOptions.isFlushAndReconnect()) {
				LOGGER.info(
						"Consumer timed out from Kafka topic [" + formatPluginName + "... ",
						te);
				if (currentBatchId > 0) {
					ingestRunData.flush();
				}
				consumeMessages(
						formatPluginName,
						avroFormatPlugin,
						ingestRunData,
						stream);
			}
			else {
				LOGGER.warn(
						"Consumer timed out from Kafka topic [" + formatPluginName + "... ",
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

		final String[] dimensionTypes = ingestOptions.getDimensionalityTypes();
		final Map<ByteArrayId, IndexWriter> writerMap = new HashMap<ByteArrayId, IndexWriter>();
		final Map<ByteArrayId, PrimaryIndex> indexMap = new HashMap<ByteArrayId, PrimaryIndex>();

		for (final String dimensionType : dimensionTypes) {
			final PrimaryIndex primaryIndex = IngestUtils.getIndex(
					ingestPlugin,
					ingestRunData.getArgs(),
					dimensionType);
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
				writerMap.keySet(),
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

	@Override
	protected void parseOptionsInternal(
			final Options options,
			CommandLine commandLine )
			throws ParseException {
		final CommandLineResult<DataStoreCommandLineOptions> dataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
				options,
				commandLine);
		dataStoreOptions = dataStoreOptionsResult.getResult();
		if (dataStoreOptionsResult.isCommandLineChange()) {
			commandLine = dataStoreOptionsResult.getCommandLine();
		}
		ingestOptions = IngestCommandLineOptions.parseOptions(commandLine);
		kafkaOptions = KafkaConsumerCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		DataStoreCommandLineOptions.applyOptions(allOptions);
		IngestCommandLineOptions.applyOptions(allOptions);
		KafkaConsumerCommandLineOptions.applyOptions(allOptions);
	}
}
