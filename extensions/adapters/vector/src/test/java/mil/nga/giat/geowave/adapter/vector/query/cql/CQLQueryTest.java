package mil.nga.giat.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class CQLQueryTest
{
	private static final NumericIndexStrategy SPATIAL_INDEX_STRATEGY = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex()
			.getIndexStrategy();
	private static final NumericIndexStrategy SPATIAL_TEMPORAL_INDEX_STRATEGY = new SpatialTemporalDimensionalityTypeProvider()
			.createPrimaryIndex()
			.getIndexStrategy();
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
		final CQLQuery query = (CQLQuery) CQLQuery.createOptimalQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
				adapter,
				null,
				null);
		final List<MultiDimensionalNumericData> constraints = query
				.getIndexConstraints(SPATIAL_TEMPORAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2,
					1.116534776001E12
				}));
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3,
					1.116538375999E12
				}));
	}

	@Test
	public void testGeoAndTemporalWithNonMatchingIndex()
			throws CQLException {
		final CQLQuery query = (CQLQuery) CQLQuery.createOptimalQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
				adapter,
				null,
				null);
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
		final CQLQuery query = (CQLQuery) CQLQuery.createOptimalQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20)",
				adapter,
				null,
				null);
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
		final CQLQuery query = (CQLQuery) CQLQuery.createOptimalQuery(
				"pid = '10'",
				adapter,
				null,
				null);
		assertTrue(query.getIndexConstraints(
				SPATIAL_INDEX_STRATEGY).isEmpty());
	}

	@Test
	public void testNoConstraintsWithTemporalIndex()
			throws CQLException {
		final CQLQuery query = (CQLQuery) CQLQuery.createOptimalQuery(
				"pid = '10'",
				adapter,
				null,
				null);
		assertTrue(query.getIndexConstraints(
				SPATIAL_TEMPORAL_INDEX_STRATEGY).isEmpty());
	}

	@Test
	public void testGeoWithTemporalIndex()
			throws CQLException {
		final CQLQuery query = (CQLQuery) CQLQuery.createOptimalQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20)",
				adapter,
				null,
				null);
		assertTrue(query.getIndexConstraints(
				SPATIAL_TEMPORAL_INDEX_STRATEGY).isEmpty());
	}

	@Test
	public void testGeoTemporalRangeWithMatchingIndex()
			throws CQLException,
			SchemaException {
		final SimpleFeatureType type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				type);
		final CQLQuery query = (CQLQuery) CQLQuery.createOptimalQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20) and start during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
				adapter,
				null,
				null);
		final List<MultiDimensionalNumericData> constraints = query
				.getIndexConstraints(SPATIAL_TEMPORAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2,
					1.116534776001E12
				}));
		assertTrue(Arrays.equals(
				constraints.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3,
					1.116538375999E12
				}));
		final CQLQuery query2 = (CQLQuery) CQLQuery.createOptimalQuery(
				"BBOX(geometry,27.20,41.30,27.30,41.20) and end during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
				adapter,
				null,
				null);
		final List<MultiDimensionalNumericData> constraints2 = query2
				.getIndexConstraints(SPATIAL_TEMPORAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints2.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2,
					1.116534776001E12
				}));
		assertTrue(Arrays.equals(
				constraints2.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3,
					1.116538375999E12
				}));

		final CQLQuery query3 = (CQLQuery) CQLQuery
				.createOptimalQuery(
						"BBOX(geometry,27.20,41.30,27.30,41.20) and (start before 2005-05-19T21:32:56Z and end after 2005-05-19T20:32:56Z)",
						adapter,
						null,
						null);
		final List<MultiDimensionalNumericData> constraints3 = query3
				.getIndexConstraints(SPATIAL_TEMPORAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints3.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2,
					1.116534776001E12
				}));
		assertTrue(Arrays.equals(
				constraints3.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3,
					1.116538375999E12
				}));

		final CQLQuery query4 = (CQLQuery) CQLQuery
				.createOptimalQuery(
						"BBOX(geometry,27.20,41.30,27.30,41.20) and (start after 2005-05-19T20:32:56Z and end after 2005-05-19T20:32:56Z)",
						adapter,
						null,
						null);
		final List<MultiDimensionalNumericData> constraints4 = query4
				.getIndexConstraints(SPATIAL_TEMPORAL_INDEX_STRATEGY);
		assertTrue(Arrays.equals(
				constraints4.get(
						0).getMinValuesPerDimension(),
				new double[] {
					27.2,
					41.2,
					1.116534776001E12
				}));
		assertTrue(Arrays.equals(
				constraints4.get(
						0).getMaxValuesPerDimension(),
				new double[] {
					27.3,
					41.3,
					9.223372036854775999E18
				}));

	}

}
