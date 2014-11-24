package mil.nga.giat.geowave.analytics.clustering;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;

import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;

public class ClusteringUtilsTest
{

	@Test
	public void testBoundingBoxCreation()
			throws ParseException {
		final Geometry geo = ClusteringUtils.createBoundingRegion("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
		final Envelope bb = geo.getEnvelopeInternal();
		assertEquals(
				40,
				bb.getMaxX(),
				0.001);
		assertEquals(
				10,
				bb.getMinX(),
				0.001);
	}

	@Test
	public void testBoundingBoxCreationFromConfig()
			throws IOException {
		final Job job = Job.getInstance();
		final SpatialQuery query = new SpatialQuery(
				ClusteringUtils.generateWorldPolygon());
		GeoWaveInputFormat.setQuery(
				job,
				query);
		GeoWaveInputFormat.addIndex(
				job,
				IndexType.SPATIAL_VECTOR.createDefaultIndex());
		assertEquals(
				"POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))",
				ClusteringUtils.getBoundingRegionForQuery(
						job,
						GeoWaveInputFormat.class));
		final GeometryFactory factory = new GeometryFactory();
		GeoWaveInputFormat.setQuery(
				job,
				new SpatialQuery(
						factory.createPolygon(new Coordinate[] {
							new Coordinate(
									1.0249,
									1.0319),
							new Coordinate(
									1.0261,
									1.0319),
							new Coordinate(
									1.0261,
									1.0331),
							new Coordinate(
									1.0249,
									1.0319)
						})));
		assertEquals(
				"POLYGON ((1.0249 1.0319, 1.0261 1.0319, 1.0261 1.0331, 1.0249 1.0319))",
				ClusteringUtils.getBoundingRegionForQuery(
						job,
						GeoWaveInputFormat.class));
	}

	



}
