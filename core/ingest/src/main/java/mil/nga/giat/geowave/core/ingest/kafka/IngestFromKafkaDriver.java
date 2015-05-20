package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import mil.nga.giat.geowave.core.ingest.AbstractIngestCommandLineDriver;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.GenericAvroSerializer;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * /** This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 * 
 * @param <I>
 *            The type for the input data
 * @param <O>
 *            The type that represents each data entry being ingested
 */
public class IngestFromKafkaDriver<I, O> extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(IngestFromKafkaDriver.class);
	private static int NUM_CONCURRENT_CONSUMERS = KafkaCommandLineOptions.DEFAULT_NUM_CONCURRENT_CONSUMERS;
	private static ArrayList<String> queue = new ArrayList<String>();

	private KafkaCommandLineOptions kafkaOptions;
	private AccumuloCommandLineOptions accumuloOptions;
	private IngestCommandLineOptions ingestOptions;
	private static ExecutorService singletonExecutor;
	private static boolean allPluginsConfiguredAndListening = false;

	public IngestFromKafkaDriver(
			final String operation ) {
		super(
				operation);
	}

	public static synchronized ExecutorService getExecutorService() {
		if ((singletonExecutor == null) || singletonExecutor.isShutdown()) {
			singletonExecutor = Executors.newFixedThreadPool(NUM_CONCURRENT_CONSUMERS);
		}
		return singletonExecutor;
	}

	public static boolean allPluginsConfiguredAndListening() {
		return allPluginsConfiguredAndListening;
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {

		AccumuloOperations operations;
		try {
			operations = accumuloOptions.getAccumuloOperations();

		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.fatal(
					"Unable to connect to Accumulo with the specified options",
					e);
			return;
		}

		final DataStore dataStore = new AccumuloDataStore(
				operations);

		addPluginsToQueue(pluginProviders);

		configureAndLaunchPlugins(
				dataStore,
				pluginProviders);

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
				LOGGER.error(e);
			}
			counter++;
		}

		if (queue.size() == 0) {
			LOGGER.info("All format plugins are now listening on Kafka topics");
			allPluginsConfiguredAndListening = true;
		}
		else {
			LOGGER.warn("Unable to setup Kafka consumers for the following format plugins:");
			for (final String formatPluginName : queue) {
				LOGGER.warn("\t[" + formatPluginName + "]");
			}
		}
	}

	private void addPluginsToQueue(
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
		for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			queue.add(pluginProvider.getIngestFormatName());
		}
	}

	private void configureAndLaunchPlugins(
			final DataStore dataStore,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
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
					final IngestRunData runData = new IngestRunData(
							adapters,
							dataStore);

					launchTopicConsumer(
							pluginProvider.getIngestFormatName(),
							avroFormatPlugin,
							runData);
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
		final Properties properties = new Properties();
		properties.put(
				"zookeeper.connect",
				KafkaCommandLineOptions.getProperties().get(
						"zookeeper.connect"));
		properties.put(
				"group.id",
				"0");

		properties.put(
				"fetch.message.max.bytes",
				KafkaCommandLineOptions.MAX_MESSAGE_FETCH_SIZE);

		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				properties));

		return consumer;
	}

	private void launchTopicConsumer(
			final String formatPluginName,
			final AvroFormatPlugin avroFormatPlugin,
			final IngestRunData ingestRunData )
			throws Exception {
		final ExecutorService executorService = getExecutorService();
		executorService.execute(new Runnable() {

			@Override
			public void run() {
				try {
					consumeFromTopic(
							formatPluginName,
							avroFormatPlugin,
							ingestRunData);
				}
				catch (final Exception e) {
					LOGGER.error(
							"Error consuming from Kafka topic [" + formatPluginName + "]",
							e);
				}
			}
		});
	}

	public void consumeFromTopic(
			final String formatPluginName,
			final AvroFormatPlugin avroFormatPlugin,
			final IngestRunData ingestRunData )
			throws Exception {

		final ConsumerConnector consumer = buildKafkaConsumer();
		if (consumer == null) {
			throw new Exception(
					"Kafka consumer connector is null, unable to create message streams");
		}
		final Map<String, Integer> topicCount = new HashMap<>();
		topicCount.put(
				formatPluginName,
				1);

		final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		final List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(formatPluginName);
		int counter = 1;
		for (final KafkaStream stream : streams) {
			final ConsumerIterator<byte[], byte[]> it = stream.iterator();
			queue.remove(formatPluginName);
			LOGGER.info("Kafka consumer setup for format [" + formatPluginName + "] against topic [" + formatPluginName + "]");
			try {
				while (it.hasNext()) {
					final byte[] msg = it.next().message();
					counter++;
					LOGGER.debug("[" + formatPluginName + "] message received");
					final I dataRecord = GenericAvroSerializer.deserialize(
							msg,
							avroFormatPlugin.getAvroSchema());

					if (dataRecord != null) {
						try {
							processMessage(
									dataRecord,
									ingestRunData,
									avroFormatPlugin);
						}
						catch (final Exception e) {
							LOGGER.error("Error processing message: " + e.getMessage());
						}
					}
				}
			}
			catch (final Exception e) {

				LOGGER.warn("Consuming from Kafka topic [" + formatPluginName + "] was interrupted... ");

			}
			finally {
				consumer.shutdown();
			}
		}
	}

	synchronized protected void processMessage(
			final I dataRecord,
			final IngestRunData ingestRunData,
			final AvroFormatPlugin<I, O> plugin )
			throws IOException {

		final Index supportedIndex = ingestOptions.getIndex(plugin.getSupportedIndices());
		if (supportedIndex == null) {
			LOGGER.error("Could not get index instance, getIndex() returned null;");
			throw new IOException(
					"Could not get index instance, getIndex() returned null");
		}
		final IndexWriter indexWriter = ingestRunData.getIndexWriter(supportedIndex);
		final Index idx = indexWriter.getIndex();
		if (idx == null) {
			LOGGER.error("Could not get index instance, getIndex() returned null;");
			throw new IOException(
					"Could not get index instance, getIndex() returned null");
		}

		final IngestPluginBase<I, O> ingestAvroPlugin = plugin.getIngestWithAvroPlugin();
		try (CloseableIterator<GeoWaveData<O>> geowaveDataIt = ingestAvroPlugin.toGeoWaveData(
				dataRecord,
				idx.getId(),
				ingestOptions.getVisibility())) {
			while (geowaveDataIt.hasNext()) {
				final GeoWaveData<O> geowaveData = geowaveDataIt.next();
				final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
				if (adapter == null) {
					LOGGER.warn("Adapter not found for " + geowaveData.getValue());
					continue;
				}
				indexWriter.write(
						adapter,
						geowaveData.getValue());
			}
		}
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		ingestOptions = IngestCommandLineOptions.parseOptions(commandLine);
		kafkaOptions = KafkaCommandLineOptions.parseOptions(commandLine);

		NUM_CONCURRENT_CONSUMERS = kafkaOptions.getKafkaNumConsumers();
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
		IngestCommandLineOptions.applyOptions(allOptions);
		KafkaCommandLineOptions.applyOptions(allOptions);
	}

}
