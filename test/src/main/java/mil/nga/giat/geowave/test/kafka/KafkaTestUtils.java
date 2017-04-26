package mil.nga.giat.geowave.test.kafka;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.ingest.operations.KafkaToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToKafkaCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class KafkaTestUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTestEnvironment.class);
	private final static String MAX_MESSAGE_BYTES = "5000000";

	protected static final File DEFAULT_LOG_DIR = new File(
			TestUtils.TEMP_DIR,
			"kafka-logs");

	public static void testKafkaStage(
			final String ingestFilePath ) {
		LOGGER.warn("Staging '" + ingestFilePath + "' to a Kafka topic - this may take several minutes...");
		final String[] args = null;
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
		final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin("gpx");

		final LocalToKafkaCommand localToKafka = new LocalToKafkaCommand();
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

	public static void testKafkaIngest(
			final DataStorePluginOptions options,
			final boolean spatialTemporal,
			final String ingestFilePath ) {
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");

		// // FIXME
		// final String[] args = StringUtils.split("-kafkaingest" +
		// " -f gpx -batchSize 1 -consumerTimeoutMs 5000 -reconnectOnTimeout
		// -groupId testGroup"
		// + " -autoOffsetReset smallest -fetchMessageMaxBytes " +
		// MAX_MESSAGE_BYTES +
		// " -zookeeperConnect " + zookeeper + " -" +

		// Ingest Formats
		final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin("gpx");

		// Indexes
		final IndexPluginOptions indexOption = new IndexPluginOptions();
		indexOption.selectPlugin((spatialTemporal ? "spatial_temporal" : "spatial"));

		// Execute Command
		final KafkaToGeowaveCommand kafkaToGeowave = new KafkaToGeowaveCommand();
		kafkaToGeowave.setPluginFormats(ingestFormatOptions);
		kafkaToGeowave.setInputIndexOptions(Arrays.asList(indexOption));
		kafkaToGeowave.setInputStoreOptions(options);
		kafkaToGeowave.getKafkaOptions().setConsumerTimeoutMs(
				"5000");
		kafkaToGeowave.getKafkaOptions().setReconnectOnTimeout(
				false);
		kafkaToGeowave.getKafkaOptions().setGroupId(
				"testGroup");
		kafkaToGeowave.getKafkaOptions().setAutoOffsetReset(
				"smallest");
		kafkaToGeowave.getKafkaOptions().setFetchMessageMaxBytes(
				MAX_MESSAGE_BYTES);
		kafkaToGeowave.getKafkaOptions().setZookeeperConnect(
				ZookeeperTestEnvironment.getInstance().getZookeeper());
		kafkaToGeowave.setParameters(
				null,
				null);

		kafkaToGeowave.execute(new ManualOperationParams());

		// Wait for ingest to complete. This works because we have set
		// Kafka Consumer to Timeout and set the timeout at 5000 ms, and
		// then not to re-connect. Since this is a unit test that should
		// be fine. Basically read all data that's in the stream and
		// finish.
		try {
			kafkaToGeowave.getDriver().waitFutures();
		}
		catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(
					e);
		}
	}

	public static KafkaConfig getKafkaBrokerConfig() {
		final Properties props = new Properties();
		props.put(
				"log.dirs",
				DEFAULT_LOG_DIR.getAbsolutePath());
		props.put(
				"zookeeper.connect",
				ZookeeperTestEnvironment.getInstance().getZookeeper());
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

}
