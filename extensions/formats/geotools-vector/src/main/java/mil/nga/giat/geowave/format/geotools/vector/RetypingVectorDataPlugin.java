package mil.nga.giat.geowave.format.geotools.vector;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public interface RetypingVectorDataPlugin
{
	public RetypingVectorDataSource getRetypingSource(
			SimpleFeatureType type );

	public static interface RetypingVectorDataSource
	{
		public SimpleFeatureType getRetypedSimpleFeatureType();

		public Object retypeAttributeValue(
				Object value,
				Name attributeName );

		public String getFeatureId(
				SimpleFeature original );
	}
}
