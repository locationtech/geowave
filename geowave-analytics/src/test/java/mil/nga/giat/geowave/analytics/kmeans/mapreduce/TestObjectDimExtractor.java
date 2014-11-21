package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import mil.nga.giat.geowave.analytics.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytics.extract.EmptyDimensionExtractor;

import com.vividsolutions.jts.geom.Geometry;

public class TestObjectDimExtractor extends
		EmptyDimensionExtractor<TestObject> implements
		DimensionExtractor<TestObject>
{
	@Override
	public String getGroupID(
			TestObject anObject ) {
		return anObject.getGroupID();
	}

	@Override
	public Geometry getGeometry(
			TestObject anObject ) {
		return anObject.geo;
	}
}
