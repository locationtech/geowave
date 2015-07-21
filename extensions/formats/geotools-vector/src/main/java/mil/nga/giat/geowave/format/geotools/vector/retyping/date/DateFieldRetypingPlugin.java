package mil.nga.giat.geowave.format.geotools.vector.retyping.date;

import java.util.Map;

import mil.nga.giat.geowave.format.geotools.vector.RetypingVectorDataPlugin;

import org.opengis.feature.simple.SimpleFeatureType;

public class DateFieldRetypingPlugin implements
		RetypingVectorDataPlugin
{

	private final DateFieldOptionProvider dateFieldOptionProvider;

	public DateFieldRetypingPlugin(
			final DateFieldOptionProvider dateFieldOptionProvider ) {
		this.dateFieldOptionProvider = dateFieldOptionProvider;
	}

	@Override
	public RetypingVectorDataSource getRetypingSource(
			final SimpleFeatureType type ) {

		final Map<String, String> fieldNameToTimestampFormat = dateFieldOptionProvider.getFieldToFormatMap();

		RetypingVectorDataSource retypingSource = null;
		if (fieldNameToTimestampFormat != null && !fieldNameToTimestampFormat.isEmpty()) {
			retypingSource = new DateFieldRetypingSource(
					type,
					fieldNameToTimestampFormat);
		}
		return retypingSource;
	}
}
