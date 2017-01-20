package mil.nga.giat.geowave.core.ingest.kafka;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import mil.nga.giat.geowave.core.ingest.avro.GenericAvroSerializer;

import org.apache.avro.specific.SpecificRecordBase;

/**
 * Default encoder used by Kafka to serialize Avro generated Java object to
 * binary. This class is specified as a property in the Kafka config setup.
 * 
 * Key: serializer.class
 * 
 * Value: mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder
 * 
 * @param <T>
 *            - Base Avro class extended by all generated class files
 */
public class AvroKafkaEncoder<T extends SpecificRecordBase> implements
		Encoder<T>
{
	private final GenericAvroSerializer<T> serializer = new GenericAvroSerializer<T>();

	public AvroKafkaEncoder(
			final VerifiableProperties verifiableProperties ) {
		// This constructor must be present to avoid runtime errors
	}

	@Override
	public byte[] toBytes(
			final T object ) {
		return serializer.serialize(
				object,
				object.getSchema());
	}
}