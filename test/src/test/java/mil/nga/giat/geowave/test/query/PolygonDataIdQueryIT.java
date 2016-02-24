package mil.nga.giat.geowave.test.query;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class PolygonDataIdQueryIT extends
		GeoWaveTestEnvironment
{
	private static final Logger LOGGER = Logger.getLogger(PolygonDataIdQueryIT.class);
	private static SimpleFeatureType simpleFeatureType;
	private static FeatureDataAdapter dataAdapter;
	private static DataStore dataStore;
	private static final String GEOMETRY_ATTRIBUTE = "geometry";
	private static final String DATA_ID = "dataId";

	@Test
	public void testPolygonDataIdQueryResults() {
		final CloseableIterator<SimpleFeature> matches = dataStore.query(
				new QueryOptions(
						dataAdapter,
						DEFAULT_SPATIAL_INDEX),
				new DataIdQuery(
						dataAdapter.getAdapterId(),
						new ByteArrayId(
								StringUtils.stringToBinary(DATA_ID))));
		int numResults = 0;
		while (matches.hasNext()) {
			matches.next();
			numResults++;
		}
		Assert.assertTrue(
				"Expected 1 result, but returned " + numResults,
				numResults == 1);
	}

	@BeforeClass
	public static void setupData()
			throws IOException {
		GeoWaveTestEnvironment.setup();
		simpleFeatureType = getSimpleFeatureType();
		dataAdapter = new FeatureDataAdapter(
				simpleFeatureType);
		dataStore = new AccumuloDataStore(
				accumuloOperations);
		ingestSampleData();
	}

	private static void ingestSampleData()
			throws IOException {
		try (@SuppressWarnings("unchecked")
		IndexWriter writer = dataStore.createIndexWriter(
				DEFAULT_SPATIAL_INDEX,
				DataStoreUtils.DEFAULT_VISIBILITY)) {
			writer.write(
					dataAdapter,
					buildSimpleFeature(
							DATA_ID,
							GeometryUtils.GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
								new Coordinate(
										1.0249,
										1.0319),
								new Coordinate(
										1.0261,
										1.0319),
								new Coordinate(
										1.0261,
										1.0323),
								new Coordinate(
										1.0249,
										1.0319)
							})));
		}
	}

	private static SimpleFeatureType getSimpleFeatureType() {
		SimpleFeatureType type = null;
		try {
			type = DataUtilities.createType(
					"data",
					GEOMETRY_ATTRIBUTE + ":Geometry");
		}
		catch (final SchemaException e) {
			LOGGER.error(
					"Unable to create SimpleFeatureType",
					e);
		}
		return type;
	}

	private static SimpleFeature buildSimpleFeature(
			final String dataId,
			final Geometry geo ) {
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				simpleFeatureType);
		builder.set(
				GEOMETRY_ATTRIBUTE,
				geo);
		return builder.buildFeature(dataId);
	}
}
