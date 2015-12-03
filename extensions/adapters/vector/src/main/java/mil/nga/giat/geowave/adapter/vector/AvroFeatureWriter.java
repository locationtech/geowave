package mil.nga.giat.geowave.adapter.vector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.simpleFeature.avro.AttributeValue;
import mil.nga.giat.geowave.adapter.vector.simpleFeature.avro.AvroSimpleFeature;
import mil.nga.giat.geowave.adapter.vector.simpleFeature.avro.FeatureDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

public class AvroFeatureWriter implements
		FieldWriter<SimpleFeature, Object>
{
	private static final Logger LOGGER = Logger.getLogger(AvroFeatureWriter.class);

	private final EncoderFactory ef = EncoderFactory.get();

	private final SpecificDatumWriter<AvroSimpleFeature> datumWriter = new SpecificDatumWriter<AvroSimpleFeature>();
	private final WKBWriter wkbWriter = new WKBWriter(
			3);

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
		catch (IOException e) {
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
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		BinaryEncoder encoder = ef.binaryEncoder(
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
			SimpleFeature sf,
			AvroSimpleFeature avroObjectToReuse,
			Map<String, String> defaultClassifications,
			String defaultClassification )
			throws IOException {
		if (sf == null) {
			throw new IOException(
					"Feature cannot be null");
		}

		if (defaultClassification == null && defaultClassifications == null) {
			throw new IOException(
					"if per attribute classifications aren't provided then a default classification must be provided");
		}

		SimpleFeatureType sft = sf.getType();
		if (avroObjectToReuse == null) {
			avroObjectToReuse = new AvroSimpleFeature();
		}

		FeatureDefinition fd = buildFeatureDefinition(
				avroObjectToReuse.getFeatureType(),
				sft,
				defaultClassifications,
				defaultClassification);
		avroObjectToReuse.setFeatureType(fd);

		AttributeValue av = buildAttributeValue(
				sf,
				sft);
		avroObjectToReuse.setValue(av);

		return serializeAvroSimpleFeature(avroObjectToReuse);
	}

	/**
	 * Add the attributes, types and classifications for the SimpleFeatureType
	 * to the provided FeatureDefinition
	 * 
	 * @param fd
	 *            - existing Feature Definition (or new one if null)
	 * @param sft
	 *            - SimpleFeatureType of the simpleFeature being serialized
	 * @param defaultClassifications
	 *            - map of attribute names to classification
	 * @param defaultClassification
	 *            - default classification if one could not be found in the map
	 * @return
	 * @throws IOException
	 */
	private FeatureDefinition buildFeatureDefinition(
			FeatureDefinition fd,
			SimpleFeatureType sft,
			Map<String, String> defaultClassifications,
			String defaultClassification )
			throws IOException {
		if (fd == null) {
			fd = new FeatureDefinition();
		}
		fd.setFeatureTypeName(sft.getTypeName());

		List<String> attributes = new ArrayList<String>(
				sft.getAttributeCount());
		List<String> types = new ArrayList<String>(
				sft.getAttributeCount());
		List<String> classifications = new ArrayList<String>(
				sft.getAttributeCount());

		for (AttributeDescriptor attr : sft.getAttributeDescriptors()) {
			String localName = attr.getLocalName();

			attributes.add(localName);
			types.add(attr.getType().getBinding().getCanonicalName());
			classifications.add(getClassification(
					localName,
					defaultClassifications,
					defaultClassification));
		}

		fd.setAttributeNames(attributes);
		fd.setAttributeTypes(types);
		fd.setAttributeDefaultClassifications(classifications);

		return fd;
	}

	/**
	 * If a classification exists for this attribute name then use it If not
	 * then use the provided default classification
	 * 
	 * @param localName
	 *            - attribute name
	 * @param defaultClassifications
	 *            - map of attribute names to classification
	 * @param defaultClassification
	 *            - default classification to use if one is not mapped for the
	 *            name provided
	 * @return
	 * @throws IOException
	 */
	private String getClassification(
			String localName,
			Map<String, String> defaultClassifications,
			String defaultClassification )
			throws IOException {
		String classification = null;

		if (defaultClassifications != null && defaultClassifications.containsKey(localName)) {
			classification = defaultClassifications.get(localName);
		}
		else {
			classification = defaultClassification;
		}

		if (classification == null) {
			throw new IOException(
					"No default classification was provided, and no classification for: '" + localName + "' was provided");
		}

		return classification;
	}

	/**
	 * Create an AttributeValue from the SimpleFeature's attributes
	 * 
	 * @param sf
	 * @param sft
	 * @return
	 */
	private AttributeValue buildAttributeValue(
			SimpleFeature sf,
			SimpleFeatureType sft ) {
		AttributeValue attributeValue = new AttributeValue();

		List<ByteBuffer> values = new ArrayList<ByteBuffer>(
				sft.getAttributeCount());

		attributeValue.setFid(sf.getID());

		for (AttributeDescriptor attr : sft.getAttributeDescriptors()) {
			Object o = sf.getAttribute(attr.getLocalName());
			byte[] bytes = null;
			if (o instanceof Geometry) {
				bytes = wkbWriter.write((Geometry) o);
			}
			else {
				FieldWriter fw = FieldUtils.getDefaultWriterForClass(attr.getType().getBinding());
				bytes = fw.writeField(o);
			}
			values.add(ByteBuffer.wrap(bytes));
		}
		attributeValue.setValues(values);

		return attributeValue;
	}
}