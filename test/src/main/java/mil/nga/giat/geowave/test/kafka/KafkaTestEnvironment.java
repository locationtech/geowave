package mil.nga.giat.geowave.test.kafka;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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
		synchronized (MUTEX) {
			args = StringUtils.split("-kafkastage -f gpx -b " + ingestFilePath + " -metadataBrokerList " + localhost + ":9092 -requestRequiredAcks 1 -producerType sync -retryBackoffMs 1000 -serializerClass mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder" + ' ');
		}

		GeoWaveMain.main(args);
	}

	protected void testKafkaIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-kafkaingest -f gpx -consumerTimeoutMs 5000 -reconnectOnTimeout -groupId testGroup -autoOffsetReset smallest -fetchMessageMaxBytes " + MAX_MESSAGE_BYTES + " -zookeeperConnect " + zookeeper + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		GeoWaveMain.main(args);
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
