package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;

public class ClusteringUtils
{

	/*
	 * null field will result in all possible entries in that field
	 */
	public static IteratorSetting createScanIterator(
			final String iterName,
			final String rowRegex,
			final String colfRegex,
			final String colqRegex,
			final String valRegex,
			final boolean orFields ) {
		final IteratorSetting iter = new IteratorSetting(
				15,
				iterName,
				RegExFilter.class);
		RegExFilter.setRegexs(
				iter,
				rowRegex,
				colfRegex,
				colqRegex,
				valRegex,
				orFields);

		return iter;
	}

	/*
	 * generate a polygon with 4 points defining a boundary of lon: -180 to 180
	 * and lat: -90 to 90 in a counter-clockwise path
	 */
	public static Polygon generateWorldPolygon() {
		final Coordinate[] coordinateArray = new Coordinate[5];
		coordinateArray[0] = new Coordinate(
				-180.0,
				-90.0);
		coordinateArray[1] = new Coordinate(
				-180.0,
				90.0);
		coordinateArray[2] = new Coordinate(
				180.0,
				90.0);
		coordinateArray[3] = new Coordinate(
				180.0,
				-90.0);
		coordinateArray[4] = new Coordinate(
				-180.0,
				-90.0);
		final CoordinateArraySequence cas = new CoordinateArraySequence(
				coordinateArray);
		final LinearRing linearRing = new LinearRing(
				cas,
				new GeometryFactory());
		return new Polygon(
				linearRing,
				null,
				new GeometryFactory());
	}

	public static SimpleFeatureType createMultiPolygonSimpleFeatureType(
			final String dataTypeId ) {
		// build a multipolygon feature type
		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(dataTypeId);
		builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference
													// system

		// add attributes in order
		builder.add(
				"geom",
				MultiPolygon.class);
		builder.add(
				"name",
				String.class);

		// build the type
		return builder.buildFeatureType();
	}

	public static SimpleFeatureType createPointSimpleFeatureType(
			final String dataTypeId ) {
		try {
			return DataUtilities.createType(
					dataTypeId, // column family in GeoWave key
					"location:Point:srid=4326," + // the default geometry
													// attribute: Point type
							"name:String"); // Point id
		}
		catch (final SchemaException e) {
			e.printStackTrace();
		}

		return null;
	}

	@SuppressWarnings("rawtypes")
	public static Integer getPointCount(Connector connector, String tableNamespace, String adapterId)
	{		
		AccumuloDataStatisticsStore statStore = new
				AccumuloDataStatisticsStore(new BasicAccumuloOperations(
						connector,
						tableNamespace));
		final DataStatistics<?> stats = statStore.getDataStatistics(
				new ByteArrayId(
						adapterId),
				CountDataStatistics.STATS_ID);
		if ((stats != null) && (stats instanceof CountDataStatistics)) {				
			return (int) ((CountDataStatistics) stats).getCount();
		}

		return null;
	}

	/*
	 * Retrieve data from GeoWave
	 */
	public static List<DataPoint> getData(
			final DataStore dataStore,
			final DataAdapter<SimpleFeature> adapter,
			final Index index,
			final Polygon polygon ) {
		final List<DataPoint> points = new ArrayList<DataPoint>();
		try {
			// extract points from GeoWave
			final CloseableIterator<?> actualResults = dataStore.query(
					adapter,
					index,
					new SpatialQuery(
							polygon));
			while (actualResults.hasNext()) {
				final Object obj = actualResults.next();
				if (obj instanceof SimpleFeature) {
					final SimpleFeature result = (SimpleFeature) obj;

					// point id stored with record id as metadata, could be just
					// id
					String metadata = result.getAttribute(
							"name").toString();
					metadata = metadata.contains("|") ? metadata.split("|")[0] : metadata;
					final Integer pointId = Integer.parseInt(metadata);

					final Geometry geometry = (Geometry) result.getDefaultGeometry();

					final Point point = geometry.getCentroid();

					final DataPoint dp = new DataPoint(
							pointId,
							point.getX(),
							point.getY(),
							pointId,
							true);
					points.add(dp);
				}
			}
			actualResults.close();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return points;
	}

	/*
	 * helper method to visualize point data
	 */
	public static void printData(
			final DataStore dataStore,
			final DataAdapter<SimpleFeature> adapter,
			final Index index,
			final Polygon polygon ) {
		try {
			// extract points from GeoWave
			final CloseableIterator<?> actualResults = dataStore.query(
					adapter,
					index,
					new SpatialQuery(
							polygon));
			while (actualResults.hasNext()) {
				final Object obj = actualResults.next();
				if (obj instanceof SimpleFeature) {
					final SimpleFeature result = (SimpleFeature) obj;

					// point id stored with record id as metadata, could be just
					// id
					String metadata = result.getAttribute(
							"name").toString();
					metadata = metadata.contains("|") ? metadata.split("|")[0] : metadata;
					final Integer pointId = Integer.parseInt(metadata);

					final Geometry geometry = (Geometry) result.getDefaultGeometry();

					final Point point = geometry.getCentroid();
					System.out.println("point id: " + pointId + ", point: " + point.toText());
				}
			}
			actualResults.close();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Retrieve data from GeoWave at specified indices in the iterator
	 */
	public static List<DataPoint> getSpecifiedPoints(
			final DataStore dataStore,
			final DataAdapter<SimpleFeature> adapter,
			final Index index,
			final List<Integer> indices ) {
		final List<DataPoint> points = new ArrayList<DataPoint>();
		try {
			int entryCounter = 0;

			// extract points from GeoWave
			final CloseableIterator<?> actualResults = dataStore.query(
					adapter,
					null);
			while (actualResults.hasNext()) {
				if (indices.contains(entryCounter)) {
					final Object obj = actualResults.next();
					if (obj instanceof SimpleFeature) {
						final SimpleFeature result = (SimpleFeature) obj;

						// point id stored as name attribute
						String metadata = result.getAttribute(
								"name").toString();
						metadata = metadata.length() > 0 ? metadata : UUID.randomUUID().toString();
						final Integer pointId = Integer.parseInt(metadata);

						final Geometry geometry = (Geometry) result.getDefaultGeometry();

						final Point point = geometry.getCentroid();

						final DataPoint dp = new DataPoint(
								pointId,
								point.getX(),
								point.getY(),
								pointId,
								true);
						points.add(dp);
					}
				}

				if (points.size() >= indices.size()) {
					break;
				}

				entryCounter++;

			}
			actualResults.close();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return points;
	}

	/*
	 * Method takes in a polygon and generates the corresponding ranges in a
	 * GeoWave spatial index
	 */
	protected static List<ByteArrayRange> getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final List<ByteArrayRange> ranges = index.getIndexStrategy().getQueryRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index.getIndexStrategy()));

		return ranges;
	}

	/*
	 * Retrieve the Point object from a GeoWave spatial index table entry
	 */
	public static Point getPointForEntry(
			final Entry<Key, Value> entry,
			final SimpleFeatureType type ) {
		final DataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
				type);

		final SimpleFeature feature = (SimpleFeature) AccumuloUtils.decodeRow(
				entry.getKey(),
				entry.getValue(),
				adapter,
				IndexType.SPATIAL_VECTOR.createDefaultIndex());

		final Geometry geometry = (Geometry) feature.getDefaultGeometry();

		return geometry.getCentroid();
	}
}
