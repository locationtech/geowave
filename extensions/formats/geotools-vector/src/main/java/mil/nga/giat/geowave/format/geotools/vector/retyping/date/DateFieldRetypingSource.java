package mil.nga.giat.geowave.format.geotools.vector.retyping.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.format.geotools.vector.AbstractFieldRetypingSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;

public class DateFieldRetypingSource extends
		AbstractFieldRetypingSource
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DateFieldRetypingSource.class);

	private final SimpleFeatureType typeIn;
	private final Map<String, String> fieldNameToTimestampFormat;
	private final ThreadLocal<Map<String, SimpleDateFormat>> fieldToFormatObjMap;

	public DateFieldRetypingSource(
			final SimpleFeatureType typeIn,
			final Map<String, String> fieldNameToTimestampFormat ) {
		this.typeIn = typeIn;
		this.fieldNameToTimestampFormat = fieldNameToTimestampFormat;

		this.fieldToFormatObjMap = new ThreadLocal<Map<String, SimpleDateFormat>>() {
			@Override
			protected Map<String, SimpleDateFormat> initialValue() {
				final Map<String, SimpleDateFormat> localFieldToFormat = new HashMap<>();
				for (Entry<String, String> entry : fieldNameToTimestampFormat.entrySet()) {
					localFieldToFormat.put(
							entry.getKey(),
							new SimpleDateFormat(
									entry.getValue()));
				}
				return localFieldToFormat;
			}
		};
	}

	@Override
	public Object retypeAttributeValue(
			Object value,
			Name attributeName ) {
		Object outValue = value;
		final String localName = attributeName.getLocalPart();
		final SimpleDateFormat formatForName = fieldToFormatObjMap.get().get(
				localName);
		if (value != null && formatForName != null) {
			try {
				outValue = formatForName.parse(value.toString());
			}
			catch (ParseException e) {
				LOGGER.error("Failed to parse: " + localName + ": " + value.toString());
			}
		}
		return outValue;
	}

	@Override
	public SimpleFeatureType getRetypedSimpleFeatureType() {
		debugType(
				"IN",
				typeIn);

		SimpleFeatureTypeBuilder typeOutBuilder = new SimpleFeatureTypeBuilder();

		// Manually set the basics and replace the date fields
		typeOutBuilder.setCRS(typeIn.getCoordinateReferenceSystem());
		typeOutBuilder.setDescription(typeIn.getDescription());
		typeOutBuilder.setName(typeIn.getName());
		for (AttributeDescriptor att : typeIn.getAttributeDescriptors()) {
			if (fieldNameToTimestampFormat.containsKey(att.getLocalName())) {
				typeOutBuilder.add(
						att.getLocalName(),
						Date.class);
			}
			else {
				typeOutBuilder.add(att);
			}
		}

		// TODO - restore this procedure when support for GeoTools 12.x is
		// dropped
		// typeOutBuilder.init(typeIn);
		// for (Entry<String, String> fieldToChange :
		// fieldNameToTimestampFormat.entrySet()) {
		// final AttributeTypeBuilder dateFieldBuilder = new
		// AttributeTypeBuilder();
		// dateFieldBuilder.setName(fieldToChange.getKey());
		// dateFieldBuilder.setBinding(Date.class);
		// typeOutBuilder.set(
		// fieldToChange.getKey(),
		// dateFieldBuilder);
		// }

		final SimpleFeatureType typeOut = typeOutBuilder.buildFeatureType();
		debugType(
				"OUT",
				typeOut);
		return typeOut;
	}

	@Override
	public String getFeatureId(
			SimpleFeature original ) {
		// We don't need to do much, we're not changing the ID
		return original.getID();
	}

	private void debugType(
			final String typeLabel,
			final SimpleFeatureType type ) {
		if (LOGGER.isDebugEnabled()) {
			final StringBuilder logBuilder = new StringBuilder();
			logBuilder.append("Type: " + typeLabel);
			for (AttributeDescriptor propDef : type.getAttributeDescriptors()) {
				logBuilder.append("\nField: " + propDef.getLocalName() + ", Type: "
						+ propDef.getType().getBinding().getSimpleName());
			}
			LOGGER.debug(logBuilder.toString());
		}
	}
}
