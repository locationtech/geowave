package mil.nga.giat.geowave.core.ingest.kafka;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class KafkaConsumerCommandLineOptions
{
	private static final KafkaCommandLineArgument[] KAFKA_CONSUMER_ARGS = new KafkaCommandLineArgument[] {
		new KafkaCommandLineArgument(
				"groupId",
				"A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same group id multiple processes indicate that they are all part of the same consumer group.",
				"group.id",
				true),
		new KafkaCommandLineArgument(
				"zookeeperConnect",
				"Specifies the ZooKeeper connection string in the form hostname:port where host and port are the host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is down you can also specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3.",
				"zookeeper.connect",
				true),
		new KafkaCommandLineArgument(
				"autoOffsetReset",
				"What to do when there is no initial offset in ZooKeeper or if an offset is out of range:\n" + "\t* smallest : automatically reset the offset to the smallest offset\n" + "\t* largest : automatically reset the offset to the largest offset\n" + "\t* anything else: throw exception to the consumer\n",
				"auto.offset.reset",
				false),
		new KafkaCommandLineArgument(
				"fetchMessageMaxBytes",
				"The number of bytes of messages to attempt to fetch for each topic-partition in each fetch request. These bytes will be read into memory for each partition, so this helps control the memory used by the consumer. The fetch request size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch.",
				"fetch.message.max.bytes",
				false),
		new KafkaCommandLineArgument(
				"consumerTimeoutMs",
				"By default, this value is -1 and a consumer blocks indefinitely if no new message is available for consumption. By setting the value to a positive integer, a timeout exception is thrown to the consumer if no message is available for consumption after the specified timeout value.",
				"consumer.timeout.ms",
				false)
	};
	private final static String RECONNECT_ON_TIMEOUT_KEY = "reconnectOnTimeout";
	private final Properties kafkaProperties;
	private final boolean flushAndReconnect;

	public KafkaConsumerCommandLineOptions(
			final Properties kafkaProperties,
			final boolean flushAndReconnect ) {
		this.kafkaProperties = kafkaProperties;
		this.flushAndReconnect = flushAndReconnect;
	}

	public Properties getProperties() {
		return kafkaProperties;
	}

	public boolean isFlushAndReconnect() {
		return flushAndReconnect;
	}

	public static void applyOptions(
			final Options allOptions ) {
		KafkaCommandLineOptions.applyOptions(allOptions);
		KafkaCommandLineOptions.applyAdditionalOptions(
				allOptions,
				KAFKA_CONSUMER_ARGS);

		final Option reconnectOnTimeoutOption = new Option(
				RECONNECT_ON_TIMEOUT_KEY,
				false,
				"This flag will flush when the consumer timeout occurs (based on kafka property 'consumer.timeout.ms') and immediately reconnect");
		reconnectOnTimeoutOption.setRequired(false);

		allOptions.addOption(reconnectOnTimeoutOption);
	}

	public static KafkaConsumerCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final KafkaCommandLineOptions baseOptions = KafkaCommandLineOptions.parseOptionsWithAdditionalArguments(
				commandLine,
				KAFKA_CONSUMER_ARGS);
		final boolean flushAndReconnect = commandLine.hasOption(RECONNECT_ON_TIMEOUT_KEY);
		return new KafkaConsumerCommandLineOptions(
				baseOptions.getProperties(),
				flushAndReconnect);
	}
}
