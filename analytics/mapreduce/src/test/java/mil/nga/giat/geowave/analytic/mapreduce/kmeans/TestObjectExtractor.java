package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;

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
