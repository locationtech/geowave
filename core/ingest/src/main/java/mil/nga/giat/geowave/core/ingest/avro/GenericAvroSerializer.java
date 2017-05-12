package mil.nga.giat.geowave.core.ingest.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic Avro serializer/deserializer, can convert Avro Java object to a byte
 * array and a byte array back to a usable Avro Java object.
 * 
 * @param <T>
 *            - Base Avro class extended by all generated class files
 */
public class GenericAvroSerializer<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GenericAvroSerializer.class);

	private static final EncoderFactory ef = EncoderFactory.get();
	private static final DecoderFactory df = DecoderFactory.get();
	private static final Map<String, SpecificDatumWriter> writers = new HashMap<>();
	private static final Map<String, SpecificDatumReader> readers = new HashMap<>();

	public GenericAvroSerializer() {}

	synchronized public static <T> byte[] serialize(
			final T avroObject,
			final Schema avroSchema ) {

		try {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			final BinaryEncoder encoder = ef.binaryEncoder(
					os,
					null);

			final String schemaName = getSchemaName(avroSchema);
			if (!writers.containsKey(schemaName)) {
				writers.put(
						schemaName,
						new SpecificDatumWriter<T>(
								avroSchema));
			}

			final SpecificDatumWriter<T> writer = writers.get(schemaName);
			writer.write(
					avroObject,
					encoder);
			encoder.flush();
			return os.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to serialize Avro record to byte[]: " + e.getMessage(),
					e);
			return null;
		}
	}

	synchronized public static <T> T deserialize(
			final byte[] avroData,
			final Schema avroSchema ) {
		try {
			final BinaryDecoder decoder = df.binaryDecoder(
					avroData,
					null);

			final String schemaName = getSchemaName(avroSchema);
			if (!readers.containsKey(schemaName)) {
				readers.put(
						schemaName,
						new SpecificDatumReader<T>(
								avroSchema));
			}
			final SpecificDatumReader<T> reader = readers.get(schemaName);
			return reader.read(
					null,
					decoder);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to deserialize byte[] to Avro object: " + e.getMessage(),
					e);
			return null;
		}
	}

	private static String getSchemaName(
			final Schema schema ) {
		return schema.getNamespace() + "." + schema.getName();
	}
}
