package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.extract.EmptyDimensionExtractor;

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
