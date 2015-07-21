package mil.nga.giat.geowave.core.ingest.kafka;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class KafkaProducerCommandLineOptions
{
	private static final KafkaCommandLineArgument[] KAFKA_PRODUCER_ARGS = new KafkaCommandLineArgument[] {
		new KafkaCommandLineArgument(
				"metadataBrokerList",
				"This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas). The socket connections for sending the actual data will be established based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a subset of brokers.",
				"metadata.broker.list",
				true),
		new KafkaCommandLineArgument(
				"requestRequiredAcks",
				"This value controls when a produce request is considered completed. Specifically, how many other brokers must have committed the data to their log and acknowledged this to the leader?",
				"request.required.acks",
				false),
		new KafkaCommandLineArgument(
				"producerType",
				"This parameter specifies whether the messages are sent asynchronously in a background thread. Valid values are (1) async for asynchronous send and (2) sync for synchronous send. By setting the producer to async we allow batching together of requests (which is great for throughput) but open the possibility of a failure of the client machine dropping unsent data.",
				"producer.type",
				false),
		new KafkaCommandLineArgument(
				"serializerClass",
				"The serializer class for messages. The default encoder takes a byte[] and returns the same byte[].",
				"serializer.class",
				false),
		new KafkaCommandLineArgument(
				"retryBackoffMs",
				"The amount of time to wait before attempting to retry a failed produce request to a given topic partition. This avoids repeated sending-and-failing in a tight loop.",
				"retry.backoff.ms",
				false)
	};
	private final Properties kafkaProperties;

	public KafkaProducerCommandLineOptions(
			final Properties kafkaProperties ) {
		this.kafkaProperties = kafkaProperties;
	}

	public Properties getProperties() {
		return kafkaProperties;
	}

	public static void applyOptions(
			final Options allOptions ) {
		KafkaCommandLineOptions.applyOptions(allOptions);
		KafkaCommandLineOptions.applyAdditionalOptions(
				allOptions,
				KAFKA_PRODUCER_ARGS);
	}

	public static KafkaProducerCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final KafkaCommandLineOptions baseOptions = KafkaCommandLineOptions.parseOptionsWithAdditionalArguments(
				commandLine,
				KAFKA_PRODUCER_ARGS);
		return new KafkaProducerCommandLineOptions(
				baseOptions.getProperties());
	}
}
