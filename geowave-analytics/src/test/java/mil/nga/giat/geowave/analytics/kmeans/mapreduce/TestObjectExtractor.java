package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import mil.nga.giat.geowave.analytics.extract.CentroidExtractor;

import com.vividsolutions.jts.geom.Point;

public class TestObjectExtractor implements
		CentroidExtractor<TestObject>
{
	@Override
	public Point getCentroid(
			TestObject anObject ) {
		return anObject.geo.getCentroid();
	}
}
