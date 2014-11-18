package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
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
	public static IteratorSetting createScanIterator(String iterName, String rowRegex, String colfRegex, String colqRegex, String valRegex, boolean orFields)
	{
		IteratorSetting iter = new IteratorSetting(15, iterName, RegExFilter.class);
		RegExFilter.setRegexs(iter, rowRegex, colfRegex, colqRegex, valRegex, orFields);	
		
		return iter;
	}
	/*
	 * generate a polygon with 4 points defining a boundary of 
	 * lon: -180 to 180 and lat: -90 to 90
	 * in a counter-clockwise path
	 */
	public static Polygon generateWorldPolygon()
	{
		Coordinate[] coordinateArray = new Coordinate[5];
		coordinateArray[0] = new Coordinate(-180.0, -90.0);
		coordinateArray[1] = new Coordinate(-180.0, 90.0);
		coordinateArray[2] = new Coordinate(180.0, 90.0);
		coordinateArray[3] = new Coordinate(180.0, -90.0);
		coordinateArray[4] = new Coordinate(-180.0, -90.0);
		CoordinateArraySequence cas = new CoordinateArraySequence(coordinateArray);
		LinearRing linearRing = new LinearRing(cas, new GeometryFactory());
		return new Polygon(linearRing, null, new GeometryFactory());
	}
	
	public static SimpleFeatureType createMultiPolygonSimpleFeatureaType(final String dataTypeId)
	{
		// build a multipolygon feature type
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(dataTypeId);
		builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference system

		// add attributes in order
		builder.add("geom", MultiPolygon.class);
		builder.add("name", String.class);

		// build the type
		return builder.buildFeatureType();
	}
	
	public static SimpleFeatureType createPointSimpleFeatureType(final String dataTypeId)
	{
		try {
			return DataUtilities.createType(
					dataTypeId,                   // column family in GeoWave key
					"location:Point:srid=4326," + // the default geometry attribute: Point type
							"name:String");       // Point id
		}
		catch (SchemaException e) {
			e.printStackTrace();
		}  
		
		return null;
	}
	
	/*
	 * Retrieve point count for the polygon space from the specified data store
	 */
	public static Integer getPointCount(DataStore dataStore, DataAdapter<SimpleFeature> adapter, Index index, Polygon polygon)
	{
		int count = 0;
		try {
			// extract points from GeoWave
			CloseableIterator<?> actualResults = dataStore.query(adapter, index, new SpatialQuery(polygon));
			while (actualResults.hasNext()) {
				final Object obj = actualResults.next();
				if (obj instanceof SimpleFeature) {
					count++;
				}
			}
			actualResults.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		return count;
	}
		
	/*
	 * Retrieve data from GeoWave
	 */
	public static List<DataPoint> getData(DataStore dataStore, DataAdapter<SimpleFeature> adapter, Index index, Polygon polygon)
	{
		List<DataPoint> points = new ArrayList<DataPoint>();
		try {
			// extract points from GeoWave
			CloseableIterator<?> actualResults = dataStore.query(adapter, index, new SpatialQuery(polygon));
			while (actualResults.hasNext()) {
				final Object obj = actualResults.next();
				if (obj instanceof SimpleFeature) {
					final SimpleFeature result = (SimpleFeature) obj;

					// point id stored with record id as metadata, could be just id
					String metadata = result.getAttribute("name").toString();
					metadata = metadata.contains("|") ? metadata.split("|")[0] : metadata;
					Integer pointId = Integer.parseInt(metadata);
					
					Geometry geometry = (Geometry) result.getDefaultGeometry();

					Point point = geometry.getCentroid();

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
		catch (IOException e) {
			e.printStackTrace();
		}

		return points;
	}
	
	/*
	 * helper method to visualize point data
	 */
	public static void printData(DataStore dataStore, DataAdapter<SimpleFeature> adapter, Index index, Polygon polygon)
	{
		try {
			// extract points from GeoWave
			CloseableIterator<?> actualResults = dataStore.query(adapter, index, new SpatialQuery(polygon));
			while (actualResults.hasNext()) {
				final Object obj = actualResults.next();
				if (obj instanceof SimpleFeature) {
					final SimpleFeature result = (SimpleFeature) obj;

					// point id stored with record id as metadata, could be just id
					String metadata = result.getAttribute("name").toString();
					metadata = metadata.contains("|") ? metadata.split("|")[0] : metadata;
					Integer pointId = Integer.parseInt(metadata);
					
					Geometry geometry = (Geometry) result.getDefaultGeometry();

					Point point = geometry.getCentroid();
					System.out.println("point id: " + pointId + ", point: " + point.toText());
				}
			}
			actualResults.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Retrieve data from GeoWave at specified indices in the iterator
	 */
	public static List<DataPoint> getSpecifiedPoints(DataStore dataStore, DataAdapter<SimpleFeature> adapter, Index index, Polygon polygon, List<Integer> indices)
	{
		List<DataPoint> points = new ArrayList<DataPoint>();
		try {
			int entryCounter = 0;
			
			// extract points from GeoWave
			CloseableIterator<?> actualResults = dataStore.query(adapter, index, new SpatialQuery(polygon));
			while (actualResults.hasNext()) {
				if(indices.contains(entryCounter))
				{
					final Object obj = actualResults.next();
					if (obj instanceof SimpleFeature) {
						final SimpleFeature result = (SimpleFeature) obj;
						
						// point id stored as name attribute
						String metadata = result.getAttribute("name").toString();
						metadata = metadata.length() > 0 ? metadata : UUID.randomUUID().toString();
						Integer pointId = Integer.parseInt(metadata);
						
						Geometry geometry = (Geometry) result.getDefaultGeometry();

						Point point = geometry.getCentroid();

						final DataPoint dp = new DataPoint(
								pointId,
								point.getX(),
								point.getY(),
								pointId,
								true);
						points.add(dp);
					}
				}
				
				if(points.size() >= indices.size())
					break;
				
				entryCounter++;
				
			}
			actualResults.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		return points;
	}
	
	/*
	 * Method takes in a polygon and generates the corresponding
	 * ranges in a GeoWave spatial index
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
	public static Point getPointForEntry(Entry<Key, Value> entry, SimpleFeatureType type)
	{  
		DataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
				type);

		SimpleFeature feature = (SimpleFeature) AccumuloUtils.decodeRow(
				entry.getKey(),
				entry.getValue(),
				adapter,
				IndexType.SPATIAL_VECTOR.createDefaultIndex());

		Geometry geometry = (Geometry) feature.getDefaultGeometry();

		return geometry.getCentroid();
	}
}
