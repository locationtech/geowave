package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.log4j.Logger;

/**
 * Generic Avro serializer/deserializer, can convert Avro Java object to a byte
 * array and a byte array back to a usable Avro Java object.
 * 
 * @param <T>
 *            - Base Avro class extended by all generated class files
 */
public class GenericAvroSerializer<T extends SpecificRecordBase>
{
	private final static Logger LOGGER = Logger.getLogger(GenericAvroSerializer.class);

	public GenericAvroSerializer() {}

	public byte[] serialize(
			final T originalSource,
			final Schema schema ) {
		try {
			final ByteArrayOutputStream out = new ByteArrayOutputStream();
			final SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(
					schema);
			final Encoder encoder = EncoderFactory.get().binaryEncoder(
					out,
					null);
			writer.write(
					originalSource,
					encoder);
			encoder.flush();

			final byte[] avroBytes = out.toByteArray();
			out.close();

			return avroBytes;
		}
		catch (final IOException e) {
			LOGGER.error("unable to serialize Avro record to byte[]: " + e.getMessage());
			return null;
		}
	}

	public T deserialize(
			final byte[] bytes,
			final Schema schema ) {
		try {
			final DatumReader<T> reader = new SpecificDatumReader<T>(
					schema);

			final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
					bytes,
					null);
			final T result = reader.read(
					null,
					decoder);

			return result;

		}
		catch (final IOException e) {
			LOGGER.error("unable to deserialize byte[] to Avro object: " + e.getMessage());
			return null;
		}

	}
}
