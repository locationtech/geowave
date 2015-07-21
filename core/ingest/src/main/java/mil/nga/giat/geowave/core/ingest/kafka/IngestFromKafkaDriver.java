package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import mil.nga.giat.geowave.core.ingest.AbstractIngestCommandLineDriver;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.GenericAvroSerializer;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
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
 * This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 * 
 */
public class IngestFromKafkaDriver extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(IngestFromKafkaDriver.class);

	private KafkaConsumerCommandLineOptions kafkaOptions;
	private AccumuloCommandLineOptions accumuloOptions;
	private IngestCommandLineOptions ingestOptions;

	public IngestFromKafkaDriver(
			final String operation ) {
		super(
				operation);
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

		final List<String> queue = new ArrayList<String>();
		addPluginsToQueue(
				pluginProviders,
				queue);

		configureAndLaunchPlugins(
				dataStore,
				pluginProviders,
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
		}
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
			final List<String> queue ) {
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
			final IngestRunData ingestRunData,
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
			final IngestRunData ingestRunData,
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
			final IngestRunData ingestRunData,
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
			final IngestRunData ingestRunData,
			final AvroFormatPlugin<T, ?> plugin )
			throws IOException {
		IngestUtils.ingest(
				dataRecord,
				ingestOptions,
				plugin.getIngestWithAvroPlugin(),
				plugin,
				ingestRunData);
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		ingestOptions = IngestCommandLineOptions.parseOptions(commandLine);
		kafkaOptions = KafkaConsumerCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
		IngestCommandLineOptions.applyOptions(allOptions);
		KafkaConsumerCommandLineOptions.applyOptions(allOptions);
	}

}
