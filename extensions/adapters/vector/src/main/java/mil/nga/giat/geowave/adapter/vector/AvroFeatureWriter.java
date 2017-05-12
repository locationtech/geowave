package mil.nga.giat.geowave.adapter.vector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.avro.AttributeValues;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeature;
import mil.nga.giat.geowave.adapter.vector.avro.FeatureDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class AvroFeatureWriter implements
		FieldWriter<SimpleFeature, Object>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AvroFeatureWriter.class);

	private final EncoderFactory ef = EncoderFactory.get();

	private final SpecificDatumWriter<AvroSimpleFeature> datumWriter = new SpecificDatumWriter<AvroSimpleFeature>();

	@Override
	public byte[] getVisibility(
			final SimpleFeature rowValue,
			final ByteArrayId fieldId,
			final Object fieldValue ) {
		return new byte[] {};
	}

	@Override
	public byte[] writeField(
			final Object fieldValue ) {
		byte[] serializedAttributes = null;
		try {
			serializedAttributes = serializeAvroSimpleFeature(
					(SimpleFeature) fieldValue,
					null,
					null,
					"");
		}
		catch (final IOException e) {
			e.printStackTrace();
			LOGGER.error(
					"Error, failed to serialize SimpleFeature with id '" + ((SimpleFeature) fieldValue).getID() + "'",
					e);
		}

		// there is no need to preface the payload with the class name and a
		// length of the class name, the implementation is assumed to be known
		// on read so we can save space on persistence
		return serializedAttributes;
	}

	// Serialization logic

	/***
	 * @param avroObject
	 *            Avro object to serialized
	 * @return byte array of serialized avro data
	 * @throws IOException
	 */
	private byte[] serializeAvroSimpleFeature(
			final AvroSimpleFeature avroObject )
			throws IOException {
		final ByteArrayOutputStream os = new ByteArrayOutputStream();
		final BinaryEncoder encoder = ef.binaryEncoder(
				os,
				null);
		datumWriter.setSchema(avroObject.getSchema());
		datumWriter.write(
				avroObject,
				encoder);
		encoder.flush();
		return os.toByteArray();
	}

	/***
	 * Converts a SimpleFeature to an avroSimpleFeature and then serializes it.
	 * 
	 * @param sf
	 *            Simple Feature to be serialized
	 * @param avroObjectToReuse
	 *            null or AvroSimpleFeature instance to be re-used. If null a
	 *            new instance will be allocated
	 * @param defaultClassifications
	 *            null map of attribute names vs. classification. if null all
	 *            values will be set to the default classification
	 * @param defaultClassification
	 *            null or default classification. if null and
	 *            defaultClassifications are not provided an exception will be
	 *            thrown
	 * @return
	 * @throws IOException
	 */
	public byte[] serializeAvroSimpleFeature(
			final SimpleFeature sf,
			AvroSimpleFeature avroObjectToReuse,
			final Map<String, String> defaultClassifications,
			final String defaultClassification )
			throws IOException {
		if (sf == null) {
			throw new IOException(
					"Feature cannot be null");
		}

		if ((defaultClassification == null) && (defaultClassifications == null)) {
			throw new IOException(
					"if per attribute classifications aren't provided then a default classification must be provided");
		}

		final SimpleFeatureType sft = sf.getType();
		if (avroObjectToReuse == null) {
			avroObjectToReuse = new AvroSimpleFeature();
		}

		final FeatureDefinition fd = AvroFeatureUtils.buildFeatureDefinition(
				avroObjectToReuse.getFeatureType(),
				sft,
				defaultClassifications,
				defaultClassification);
		avroObjectToReuse.setFeatureType(fd);

		final AttributeValues av = AvroFeatureUtils.buildAttributeValue(
				sf,
				sft);
		avroObjectToReuse.setValue(av);

		return serializeAvroSimpleFeature(avroObjectToReuse);
	}
}