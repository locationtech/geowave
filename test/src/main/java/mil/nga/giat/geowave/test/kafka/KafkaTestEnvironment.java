package mil.nga.giat.geowave.test.kafka;

import java.io.File;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.ingest.kafka.KafkaCommandLineOptions;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

abstract public class KafkaTestEnvironment<I> extends
		GeoWaveTestEnvironment

{
	private final static Logger LOGGER = Logger.getLogger(KafkaTestEnvironment.class);

	protected static KafkaServerStartable kafkaServer;
	protected static final File DEFAULT_LOG_DIR = new File(
			TEMP_DIR,
			"kafka-logs");

	protected void testKafkaStage(
			final String ingestFilePath ) {
		LOGGER.warn("Staging '" + ingestFilePath + "' to a Kafka topic - this may take several minutes...");
		String[] args = null;
		synchronized (MUTEX) {
			args = StringUtils.split(
					"-kafkastage -f gpx -b " + ingestFilePath,
					' ');
		}

		GeoWaveMain.main(args);
	}

	protected void testKafkaIngest(
			final IndexType indexType,
			final String ingestFilePath ) {

		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-kafkaingest -f gpx -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		GeoWaveMain.main(args);
	}

	@BeforeClass
	public static void setupKafkaServer()
			throws Exception {
		LOGGER.info("Starting up Kafka Server...");
		final String zkConnection = miniAccumulo.getZooKeepers();
		final boolean success = DEFAULT_LOG_DIR.mkdir();
		if (!success) {
			LOGGER.warn("Unable to create Kafka log dir [" + DEFAULT_LOG_DIR.getAbsolutePath() + "]");
		}
		final KafkaConfig config = getKafkaBrokerConfig(zkConnection);
		kafkaServer = new KafkaServerStartable(
				config);

		kafkaServer.startup();

		// setup producer props
		setupKafkaProducerProps(zkConnection);
	}

	private static KafkaConfig getKafkaBrokerConfig(
			final String zkConnectString ) {

		final Properties props = new Properties();
		props.put(
				"log.dirs",
				DEFAULT_LOG_DIR.getAbsolutePath());
		props.put(
				"zookeeper.connect",
				zkConnectString);
		props.put(
				"broker.id",
				"0");
		props.put(
				"message.max.bytes",
				"5000000");
		props.put(
				"replica.fetch.max.bytes",
				"5000000");
		props.put(
				"log.flush.interval.messages",
				"1");
		props.put(
				"num.partitions",
				"1");
		return new KafkaConfig(
				props);
	}

	protected Producer<String, I> setupProducer() {
		final Properties props = new Properties();
		props.put(
				"metadata.broker.list",
				"localhost:9092");

		props.put(
				"serializer.class",
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");
		final ProducerConfig config = new ProducerConfig(
				props);

		return new Producer<String, I>(
				config);
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

	private static void setupKafkaProducerProps(
			final String zkConnectString ) {
		System.getProperties().put(
				"metadata.broker.list",
				"localhost:9092");
		System.getProperties().put(
				"zookeeper.connect",
				zkConnectString);
		System.getProperties().put(
				"message.max.bytes",
				"5000000");
		System.getProperties().put(
				"serializer.class",
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");

		KafkaCommandLineOptions.getProperties().put(
				"metadata.broker.list",
				"localhost:9092");
		KafkaCommandLineOptions.getProperties().put(
				"zookeeper.connect",
				zkConnectString);
		KafkaCommandLineOptions.getProperties().put(
				"max.message.size",
				"5000000");
		KafkaCommandLineOptions.getProperties().put(
				"serializer.class",
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");
	}

}
