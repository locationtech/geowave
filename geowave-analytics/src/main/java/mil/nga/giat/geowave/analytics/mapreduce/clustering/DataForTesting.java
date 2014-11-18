package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;


public class DataForTesting
{
	public static void doit(String tableNamespace)
	{
		/*
		 * Data is defined as 4 clusters of 15 points each
		 *     XXXXX          XXXXX       
		 *     XXXXX          XXXXX   
		 *     XXXXX          XXXXX
		 *        
		 *     XXXXX          XXXXX      
		 *     XXXXX          XXXXX    
		 *     XXXXX          XXXXX
		 * therefore, clustering should give 4 clusters as the optimum result.
		 */
		List<Coordinate> data = new ArrayList<Coordinate>();
		
		// upper left clump
		for(double lon = -45.2; lon <= -44.8; lon += 0.1)
		{
			for(double lat = 14.9; lat <= 15.1; lat += 0.1)
			{
				data.add(new Coordinate(lon, lat));
			}
		}
		
		// upper right clump
		for(double lon = 44.8; lon <= 45.2; lon += 0.1)
		{
			for(double lat = 14.9; lat <= 15.1; lat += 0.1)
			{
				data.add(new Coordinate(lon, lat));
			}
		}
		
		// lower left clump
		for(double lon = -45.2; lon <= -44.8; lon += 0.1)
		{
			for(double lat = -15.1; lat <= -14.9; lat += 0.1)
			{
				data.add(new Coordinate(lon, lat));
			}
		}
		
		// lower right clump
		for(double lon = 44.8; lon <= 45.2; lon += 0.1)
		{
			for(double lat = -15.1; lat <= -14.9; lat += 0.1)
			{
				data.add(new Coordinate(lon, lat));
			}
		}
		
		Instance zookeeperInstance = new ZooKeeperInstance("geowave", "geowave-master:2181,geowave-node1:2181,geowave-node2:2181");
		try {
			Connector accumuloConnector = zookeeperInstance.getConnector("root", new PasswordToken("geowave"));
			System.out.println("Connected to Accumulo!");
			DataStore dataStore = new AccumuloDataStore(new BasicAccumuloOperations(accumuloConnector, tableNamespace));	
			Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();		
	    	SimpleFeatureType type = DataUtilities.createType("Location",
	                "location:Point:srid=4326," + // <- the geometry attribute: Point type
	                        "name:String");	 
	    	WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(type);
			SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(type);
			
			Integer idCounter = 0;
			for(Coordinate coord : data)
			{			
				Coordinate[] coords = {coord};
				CoordinateArraySequence cas = new CoordinateArraySequence(coords);
				Point point = new Point(cas, new GeometryFactory());
				
				featureBuilder.add(point);
				featureBuilder.add(idCounter.toString()); // needs to be unique point id
				SimpleFeature feature = featureBuilder.buildFeature(null);
				dataStore.ingest(adapter, index, feature);
				featureBuilder.reset();

				idCounter++;
			}
			
		} catch (AccumuloException | SchemaException | AccumuloSecurityException e) {
			e.printStackTrace();
		} 
	}

	public static void main(
			String[] args ) {
		String tableNamespace = "clustering_data_temp";
		if(args.length == 1)
		{
			tableNamespace = args[0];
		}
		
		DataForTesting.doit(tableNamespace);

	}

}
