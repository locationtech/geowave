package mil.nga.giat.geowave.adapter.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

import mil.nga.giat.geowave.adapter.vector.avro.AttributeValues;
import mil.nga.giat.geowave.adapter.vector.avro.FeatureDefinition;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class AvroFeatureUtils
{
	private static final WKBWriter WKB_WRITER = new WKBWriter(
			3);

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
	public static FeatureDefinition buildFeatureDefinition(
			FeatureDefinition fd,
			final SimpleFeatureType sft,
			final Map<String, String> defaultClassifications,
			final String defaultClassification )
			throws IOException {
		if (fd == null) {
			fd = new FeatureDefinition();
		}
		fd.setFeatureTypeName(sft.getTypeName());

		final List<String> attributes = new ArrayList<String>(
				sft.getAttributeCount());
		final List<String> types = new ArrayList<String>(
				sft.getAttributeCount());
		final List<String> classifications = new ArrayList<String>(
				sft.getAttributeCount());

		for (final AttributeDescriptor attr : sft.getAttributeDescriptors()) {
			final String localName = attr.getLocalName();

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
	private static String getClassification(
			final String localName,
			final Map<String, String> defaultClassifications,
			final String defaultClassification )
			throws IOException {
		String classification = null;

		if ((defaultClassifications != null) && defaultClassifications.containsKey(localName)) {
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
	public static AttributeValues buildAttributeValue(
			final SimpleFeature sf,
			final SimpleFeatureType sft ) {
		final AttributeValues attributeValue = new AttributeValues();

		final List<ByteBuffer> values = new ArrayList<ByteBuffer>(
				sft.getAttributeCount());

		attributeValue.setFid(sf.getID());

		for (final AttributeDescriptor attr : sft.getAttributeDescriptors()) {
			final Object o = sf.getAttribute(attr.getLocalName());
			byte[] bytes = null;
			if (o instanceof Geometry) {
				bytes = WKB_WRITER.write((Geometry) o);
			}
			else {
				final FieldWriter fw = FieldUtils.getDefaultWriterForClass(attr.getType().getBinding());
				bytes = fw.writeField(o);
			}
			values.add(ByteBuffer.wrap(bytes));
		}
		attributeValue.setValues(values);

		return attributeValue;
	}
}
