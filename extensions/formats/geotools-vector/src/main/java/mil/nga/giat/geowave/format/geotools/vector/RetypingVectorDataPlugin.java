package mil.nga.giat.geowave.format.geotools.vector;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public interface RetypingVectorDataPlugin
{
	public RetypingVectorDataSource getRetypingSource(
			SimpleFeatureType type );

	public static interface RetypingVectorDataSource
	{
		public SimpleFeatureType getRetypedSimpleFeatureType();

		public SimpleFeature getRetypedSimpleFeature(
				SimpleFeatureBuilder retypeBuilder,
				SimpleFeature original );
	}
}
