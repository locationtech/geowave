package mil.nga.giat.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

public class CQLQueryTest
{
	private static final NumericIndexStrategy SPATIAL_INDEX_STRATEGY = new SpatialDimensionalityTypeProvider().createPrimaryIndex().getIndexStrategy();
	private static final NumericIndexStrategy SPATIAL_TEMPORAL_INDEX_STRATEGY = new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex().getIndexStrategy();
	SimpleFeatureType type;
	FeatureDataAdapter adapter;

	@Before
	public void init()
			throws SchemaException {
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,pid:String");
		adapter = new FeatureDataAdapter(
				type);
	}

	@Test
	public void testGeoAndTemporalWithMatchingIndex()
			throws CQLException {
		final CQLQuery query = new CQLQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
				adapter);
		final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(SPATIAL_TEMPORAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2,
					1.116534776E12
				}));
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3,
					1.116538376E12
				}));
	}

	@Test
	public void testGeoAndTemporalWithNonMatchingIndex()
			throws CQLException {
		final CQLQuery query = new CQLQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
				adapter);
		final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(SPATIAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2
				}));
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3
				}));
	}

	@Test
	public void testGeoWithMatchingIndex()
			throws CQLException {
		final CQLQuery query = new CQLQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20)",
				adapter);
		final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(SPATIAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2
				}));
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3
				}));
	}

	@Test
	public void testNoConstraintsWithGeoIndex()
			throws CQLException {
		final CQLQuery query = new CQLQuery(
				"pid = '10'",
				adapter);
		assertTrue(query.getIndexConstraints(
				SPATIAL_INDEX_STRATEGY).isEmpty());
	}

	@Test
	public void testNoConstraintsWithTemporalIndex()
			throws CQLException {
		final CQLQuery query = new CQLQuery(
				"pid = '10'",
				adapter);
		assertTrue(query.getIndexConstraints(
				SPATIAL_TEMPORAL_INDEX_STRATEGY).isEmpty());
	}

	@Test
	public void testGeoWithTemporalIndex()
			throws CQLException {
		final CQLQuery query = new CQLQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20)",
				adapter);
		assertTrue(query.getIndexConstraints(
				SPATIAL_TEMPORAL_INDEX_STRATEGY).isEmpty());
	}

}
