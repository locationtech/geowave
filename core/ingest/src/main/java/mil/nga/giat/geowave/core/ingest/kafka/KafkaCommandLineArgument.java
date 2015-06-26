package mil.nga.giat.geowave.core.ingest.kafka;

public class KafkaCommandLineArgument
{
	private final String argName;
	private final String argDescription;
	private final String kafkaParamName;
	private final boolean required;

	public KafkaCommandLineArgument(
			final String argName,
			final String argDescription,
			final String kafkaParamName,
			final boolean required ) {
		this.argName = argName;
		this.argDescription = "See Kafka documention for '" + kafkaParamName + "'" + argDescription;
		this.kafkaParamName = kafkaParamName;
		this.required = required;
	}

	public String getArgName() {
		return argName;
	}

	public String getArgDescription() {
		return argDescription;
	}

	public String getKafkaParamName() {
		return kafkaParamName;
	}

	public boolean isRequired() {
		return required;
	}

}
