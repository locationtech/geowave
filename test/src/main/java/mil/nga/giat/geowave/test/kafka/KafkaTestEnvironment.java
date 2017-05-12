package mil.nga.giat.geowave.test.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import mil.nga.giat.geowave.test.TestEnvironment;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class KafkaTestEnvironment implements
		TestEnvironment

{
	private static KafkaTestEnvironment singletonInstance;

	public static synchronized KafkaTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new KafkaTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTestEnvironment.class);

	private KafkaServerStartable kafkaServer;

	private KafkaTestEnvironment() {}

	@Override
	public void setup()
			throws Exception {
		LOGGER.info("Starting up Kafka Server...");
		final boolean success = KafkaTestUtils.DEFAULT_LOG_DIR.mkdir();
		if (!success) {
			LOGGER.warn("Unable to create Kafka log dir [" + KafkaTestUtils.DEFAULT_LOG_DIR.getAbsolutePath() + "]");
		}
		final KafkaConfig config = KafkaTestUtils.getKafkaBrokerConfig();
		kafkaServer = new KafkaServerStartable(
				config);

		kafkaServer.startup();
		Thread.sleep(3000);
	}

	@Override
	public void tearDown()
			throws Exception {
		LOGGER.info("Shutting down Kafka Server...");
		kafkaServer.shutdown();

		final boolean success = KafkaTestUtils.DEFAULT_LOG_DIR.delete();
		if (!success) {
			LOGGER.warn("Unable to delete Kafka log dir [" + KafkaTestUtils.DEFAULT_LOG_DIR.getAbsolutePath() + "]");
		}
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {
			ZookeeperTestEnvironment.getInstance()
		};
	}
}
