package mil.nga.giat.geowave.test.kafka;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

abstract public class KafkaTestEnvironment extends
		GeoWaveTestEnvironment

{
	private final static Logger LOGGER = Logger.getLogger(KafkaTestEnvironment.class);

	protected static String KAFKA_TEST_TOPIC = "gpxtesttopic";
	protected static KafkaServerStartable kafkaServer;
	protected static TestingServer zkServer;
	protected static ZkClient zkClient;

	protected void testKafkaStage(
			final String ingestFilePath ) {
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		String[] args = null;
		synchronized (MUTEX) {
			args = StringUtils.split(
					"-kafkastage -kafkatopic " + KAFKA_TEST_TOPIC + " -f gpx -b " + ingestFilePath,
					' ');
		}

		GeoWaveMain.main(args);
	}

	@BeforeClass
	public static void setupKafkaServer()
			throws Exception {

		GeoWaveTestEnvironment.setup();
		zkServer = new TestingServer();
		final KafkaConfig config = getKafkaConfig(zkServer.getConnectString());
		kafkaServer = new KafkaServerStartable(
				config);
		zkClient = new ZkClient(
				zkServer.getConnectString());

		kafkaServer.startup();

		AdminUtils.createTopic(
				zkClient,
				KAFKA_TEST_TOPIC,
				1,
				1,
				new Properties());
	}

	@AfterClass
	public static void stopKafkaServer()
			throws Exception {

		AdminUtils.deleteTopic(
				zkClient,
				KAFKA_TEST_TOPIC);

		kafkaServer.shutdown();
		zkServer.stop();

	}

	private static KafkaConfig getKafkaConfig(
			final String zkConnectString ) {

		final Properties config = new Properties();
		config.put(
				"metadata.broker.list",
				"localhost:9092");
		System.getProperties().put(
				"metadata.broker.list",
				"localhost:9092");
		config.put(
				"zookeeper.hosts",
				zkConnectString);
		System.getProperties().put(
				"zookeeper.hosts",
				zkConnectString);
		config.put(
				"zookeeper.connect",
				zkConnectString);
		System.getProperties().put(
				"zookeeper.connect",
				zkConnectString);
		config.put(
				"broker.id",
				"1");
		System.getProperties().put(
				"broker.id",
				"1");
		config.put(
				"serializer.class",
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");
		System.getProperties().put(
				"serializer.class",
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");

		final KafkaConfig kafkaConfig = new KafkaConfig(
				config);

		return kafkaConfig;
	}

}
