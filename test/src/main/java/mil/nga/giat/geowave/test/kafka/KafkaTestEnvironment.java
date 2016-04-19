package mil.nga.giat.geowave.test.kafka;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.ingest.operations.KafkaToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToKafkaCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

abstract public class KafkaTestEnvironment<I> extends
		GeoWaveTestEnvironment

{
	private final static Logger LOGGER = Logger.getLogger(KafkaTestEnvironment.class);
	private final static String MAX_MESSAGE_BYTES = "5000000";

	protected static KafkaServerStartable kafkaServer;
	protected static final File DEFAULT_LOG_DIR = new File(
			TEMP_DIR,
			"kafka-logs");

	protected void testKafkaStage(
			final String ingestFilePath ) {
		LOGGER.warn("Staging '" + ingestFilePath + "' to a Kafka topic - this may take several minutes...");
		String[] args = null;
		String localhost = "localhost";
		try {
			localhost = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		}
		catch (final UnknownHostException e) {
			LOGGER.warn(
					"unable to get canonical hostname for localhost",
					e);
		}

		// Ingest Formats
		IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin("gpx");

		LocalToKafkaCommand localToKafka = new LocalToKafkaCommand();
		localToKafka.setParameters(ingestFilePath);
		localToKafka.setPluginFormats(ingestFormatOptions);
		localToKafka.getKafkaOptions().setMetadataBrokerList(
				localhost + ":9092");
		localToKafka.getKafkaOptions().setRequestRequiredAcks(
				"1");
		localToKafka.getKafkaOptions().setProducerType(
				"sync");
		localToKafka.getKafkaOptions().setRetryBackoffMs(
				"1000");
		localToKafka.getKafkaOptions().setSerializerClass(
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");
		localToKafka.execute(new ManualOperationParams());
	}

	protected void testKafkaIngest(
			final boolean spatialTemporal,
			final String ingestFilePath ) {
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");

		// // FIXME
		// final String[] args = StringUtils.split("-kafkaingest" +
		// " -f gpx -batchSize 1 -consumerTimeoutMs 5000 -reconnectOnTimeout -groupId testGroup"
		// + " -autoOffsetReset smallest -fetchMessageMaxBytes " +
		// MAX_MESSAGE_BYTES +
		// " -zookeeperConnect " + zookeeper + " -" +

		// Ingest Formats
		IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin("gpx");

		// Indexes
		IndexPluginOptions indexOption = new IndexPluginOptions();
		indexOption.selectPlugin((spatialTemporal ? "spatial_temporal" : "spatial"));

		// Execute Command
		KafkaToGeowaveCommand kafkaToGeowave = new KafkaToGeowaveCommand();
		kafkaToGeowave.setPluginFormats(ingestFormatOptions);
		kafkaToGeowave.setInputIndexOptions(Arrays.asList(indexOption));
		kafkaToGeowave.setInputStoreOptions(getAccumuloStorePluginOptions(TEST_NAMESPACE));
		kafkaToGeowave.getKafkaOptions().setBatchSize(
				1);
		kafkaToGeowave.getKafkaOptions().setConsumerTimeoutMs(
				"5000");
		kafkaToGeowave.getKafkaOptions().setReconnectOnTimeout(
				true);
		kafkaToGeowave.getKafkaOptions().setGroupId(
				"testGroup");
		kafkaToGeowave.getKafkaOptions().setAutoOffsetReset(
				"smallest");
		kafkaToGeowave.getKafkaOptions().setFetchMessageMaxBytes(
				MAX_MESSAGE_BYTES);
		kafkaToGeowave.getKafkaOptions().setZookeeperConnect(
				zookeeper);
		kafkaToGeowave.setParameters(
				null,
				null);

		kafkaToGeowave.execute(new ManualOperationParams());
	}

	@BeforeClass
	public static void setupKafkaServer()
			throws Exception {
		LOGGER.info("Starting up Kafka Server...");
		final boolean success = DEFAULT_LOG_DIR.mkdir();
		if (!success) {
			LOGGER.warn("Unable to create Kafka log dir [" + DEFAULT_LOG_DIR.getAbsolutePath() + "]");
		}
		final KafkaConfig config = getKafkaBrokerConfig();
		kafkaServer = new KafkaServerStartable(
				config);

		kafkaServer.startup();
		Thread.sleep(3000);
	}

	private static KafkaConfig getKafkaBrokerConfig() {
		final Properties props = new Properties();
		props.put(
				"log.dirs",
				DEFAULT_LOG_DIR.getAbsolutePath());
		props.put(
				"zookeeper.connect",
				zookeeper);
		props.put(
				"broker.id",
				"0");
		props.put(
				"port",
				"9092");
		props.put(
				"message.max.bytes",
				MAX_MESSAGE_BYTES);
		props.put(
				"replica.fetch.max.bytes",
				MAX_MESSAGE_BYTES);
		props.put(
				"num.partitions",
				"1");
		return new KafkaConfig(
				props);
	}

	@AfterClass
	public static void stopKafkaServer()
			throws Exception {
		LOGGER.info("Shutting down Kafka Server...");
		kafkaServer.shutdown();

		final boolean success = DEFAULT_LOG_DIR.delete();
		if (!success) {
			LOGGER.warn("Unable to delete Kafka log dir [" + DEFAULT_LOG_DIR.getAbsolutePath() + "]");
		}
	}
}
