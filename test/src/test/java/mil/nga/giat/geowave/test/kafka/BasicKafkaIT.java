package mil.nga.giat.geowave.test.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.format.gpx.GpxTrack;

import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class BasicKafkaIT extends
		KafkaTestBase<GpxTrack>
{
	private static final Index INDEX = IndexType.SPATIAL_VECTOR.createDefaultIndex();
	private static final Map<ByteArrayId, Integer> EXPECTED_COUNT_PER_ADAPTER_ID = new HashMap<ByteArrayId, Integer>();
	static {
		EXPECTED_COUNT_PER_ADAPTER_ID.put(
				new ByteArrayId(
						"gpxpoint"),
				137291);
		EXPECTED_COUNT_PER_ADAPTER_ID.put(
				new ByteArrayId(
						"gpxtrack"),
				257);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBasicIngestGpx()
			throws Exception {
		testKafkaStage(OSM_GPX_INPUT_DIR);
		testKafkaIngest(
				IndexType.SPATIAL_VECTOR,
				OSM_GPX_INPUT_DIR);
		// wait a sufficient time for consumers to ingest all of the data
		Thread.sleep(60000);
		final DataStatisticsStore statsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);
		final AdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);
		int adapterCount = 0;
		try (CloseableIterator<DataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final FeatureDataAdapter adapter = (FeatureDataAdapter) adapterIterator.next();

				// query by the full bounding box, make sure there is more than
				// 0 count and make sure the count matches the number of results
				final BoundingBoxDataStatistics<?> bboxStat = (BoundingBoxDataStatistics<SimpleFeature>) statsStore.getDataStatistics(
						adapter.getAdapterId(),
						FeatureBoundingBoxStatistics.composeId(adapter.getType().getGeometryDescriptor().getLocalName()));
				final CountDataStatistics<?> countStat = (CountDataStatistics<SimpleFeature>) statsStore.getDataStatistics(
						adapter.getAdapterId(),
						CountDataStatistics.STATS_ID);
				// then query it
				final GeometryFactory factory = new GeometryFactory();
				final Envelope env = new Envelope(
						bboxStat.getMinX(),
						bboxStat.getMaxX(),
						bboxStat.getMinY(),
						bboxStat.getMaxY());
				final Geometry spatialFilter = factory.toGeometry(env);
				final Query query = new SpatialQuery(
						spatialFilter);
				final int resultCount = testQuery(
						adapter,
						query);
				assertTrue(
						"'" + adapter.getAdapterId().getString() + "' adapter must have at least one element in its statistic",
						countStat.getCount() > 0);
				assertEquals(
						"'" + adapter.getAdapterId().getString() + "' adapter should have the same results from a spatial query of '" + env + "' as its total count statistic",
						countStat.getCount(),
						resultCount);
				assertEquals(
						"'" + adapter.getAdapterId().getString() + "' adapter entries ingested does not match expected count",
						EXPECTED_COUNT_PER_ADAPTER_ID.get(adapter.getAdapterId()),
						new Integer(
								resultCount));
				adapterCount++;
			}
		}
		assertTrue(
				"There should be exactly two adapters",
				(adapterCount == 2));
	}

	private int testQuery(
			final DataAdapter<?> adapter,
			final Query query )
			throws Exception {
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);

		final CloseableIterator<?> accumuloResults = geowaveStore.query(
				adapter,
				INDEX,
				query);

		int resultCount = 0;
		while (accumuloResults.hasNext()) {
			accumuloResults.next();

			resultCount++;
		}
		accumuloResults.close();

		return resultCount;

	}
}
