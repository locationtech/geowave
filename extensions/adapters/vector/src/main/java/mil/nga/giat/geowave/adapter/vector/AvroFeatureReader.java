package mil.nga.giat.geowave.adapter.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.simpleFeature.avro.AttributeValue;
import mil.nga.giat.geowave.adapter.vector.simpleFeature.avro.AvroSimpleFeature;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

public class AvroFeatureReader implements
		FieldReader<Object>
{
	private final static Logger LOGGER = Logger.getLogger(AvroFeatureReader.class);

	private final DecoderFactory df = DecoderFactory.get();
	private final SpecificDatumReader<AvroSimpleFeature> datumReader = new SpecificDatumReader<AvroSimpleFeature>();
	private final WKBReader wkbReader = new WKBReader();

	@Override
	public Object readField(
			final byte[] fieldData ) {
		SimpleFeature deserializedSimpleFeature = null;
		try {
			deserializedSimpleFeature = deserializeAvroSimpleFeature(fieldData);
		}
		catch (Exception e) {
			LOGGER.error(
					"Unable to deserialize SimpleFeature",
					e);
		}

		return deserializedSimpleFeature;
	}

	// Deserialization logic

	/***
	 * Deserialize byte stream into an AvroSimpleFeature
	 * 
	 * @param avroData
	 *            serialized bytes of AvroSimpleFeature
	 * @param avroObjectToReuse
	 *            null or AvroSimpleFeature instance to be re-used. If null a
	 *            new object will be allocated.
	 * @return instance of AvroSimpleFeature with values parsed from avroData
	 * @throws IOException
	 */
	private AvroSimpleFeature deserializeASF(
			final byte[] avroData,
			AvroSimpleFeature avroObjectToReuse )
			throws IOException {
		BinaryDecoder decoder = df.binaryDecoder(
				avroData,
				null);
		if (avroObjectToReuse == null) {
			avroObjectToReuse = new AvroSimpleFeature();
		}

		datumReader.setSchema(avroObjectToReuse.getSchema());
		return datumReader.read(
				avroObjectToReuse,
				decoder);
	}

	/***
	 * Deserialize byte array into an AvroSimpleFeature then convert to a
	 * SimpleFeature
	 * 
	 * @param avroData
	 *            serialized bytes of a AvroSimpleFeature
	 * @return Collection of GeoTools SimpleFeature instances.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws ParseException
	 */
	public SimpleFeature deserializeAvroSimpleFeature(
			final byte[] avroData )
			throws IOException,
			ClassNotFoundException,
			ParseException {
		// Deserialize
		AvroSimpleFeature sfc = deserializeASF(
				avroData,
				null);

		// Convert
		SimpleFeature simpleFeature = null;

		SimpleFeatureTypeBuilder sftb = new SimpleFeatureTypeBuilder();
		sftb.setCRS(GeoWaveGTDataStore.DEFAULT_CRS);
		sftb.setName(sfc.getFeatureType().getFeatureTypeName());
		List<String> featureTypes = sfc.getFeatureType().getAttributeTypes();
		List<String> featureNames = sfc.getFeatureType().getAttributeNames();
		for (int i = 0; i < sfc.getFeatureType().getAttributeNames().size(); i++) {
			String type = featureTypes.get(i);
			String name = featureNames.get(i);
			Class<?> c = Class.forName(type);
			sftb.add(
					name,
					c);
		}

		SimpleFeatureType sft = sftb.buildFeatureType();
		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(
				sft);

		AttributeValue attributeValue = sfc.getValue();

		// null values should still take a place in the array - check
		Preconditions.checkArgument(featureNames.size() == attributeValue.getValues().size());
		for (int i = 0; i < attributeValue.getValues().size(); i++) {
			ByteBuffer val = attributeValue.getValues().get(
					i);

			if (featureTypes.get(
					i).equals(
					"com.vividsolutions.jts.geom.Geometry")) {
				sfb.add(wkbReader.read(val.array()));
			}
			else {
				FieldReader<?> fr = FieldUtils.getDefaultReaderForClass(Class.forName(featureTypes.get(i)));
				sfb.add(fr.readField(val.array()));
			}
		}

		simpleFeature = sfb.buildFeature(attributeValue.getFid());
		return simpleFeature;
	}
}