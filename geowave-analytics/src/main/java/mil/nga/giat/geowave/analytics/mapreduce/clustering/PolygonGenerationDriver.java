package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

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
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

public class PolygonGenerationDriver
{
	// configuration for Accumulo
	private final String instanceName;
	private final String zooservers;
	private final String user;
	private final String password;
	private Instance zookeeperInstance;
	private Connector accumuloConnector;
	private final String tempKMeansTableNamespace;

	private String outputTableNamespace;

	/**
	 * @param instanceName
	 *            String Name of Accumulo instance
	 * @param zooservers
	 *            String CSV of zookeeper addresses
	 * @param user
	 *            String User for Accumulo instance
	 * @param password
	 *            String Password for access
	 * @param dataTableNamespace
	 *            String GeoWave namespace for input data table, only run data
	 *            shall be stored in this table
	 * @param tempKMeansTableNamespace
	 *            String Namespace for temporary GeoWave table that will be
	 *            destroyed upon job completion
	 */
	public PolygonGenerationDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String tempKMeansTableNamespace,
			String outputTableNamespace) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.tempKMeansTableNamespace = tempKMeansTableNamespace;
		this.outputTableNamespace = outputTableNamespace;

		connectToAccumulo();
	}

	public PolygonGenerationDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String tempKMeansTableNamespace,
			String outputTableNamespace,
			final Connector accumuloConnector ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.tempKMeansTableNamespace = tempKMeansTableNamespace;
		this.outputTableNamespace = outputTableNamespace;

		this.accumuloConnector = accumuloConnector;
	}

	@SuppressWarnings("deprecation")
	protected void connectToAccumulo() {
		zookeeperInstance = new ZooKeeperInstance(
				instanceName,
				zooservers);

		try {
			accumuloConnector = zookeeperInstance.getConnector(
					user,
					password);
		}
		catch (final AccumuloException e) {
			e.printStackTrace();
		}
		catch (final AccumuloSecurityException e) {
			e.printStackTrace();
		}
		;
	}
	
	public String generatePolygons(String clusterAssignmentRowId)
	{
		System.out.println("Starting polygon generation...");

		final UUID uuid = UUID.randomUUID();
		String outputRowId = "Polygons_" + uuid.toString();
		
		try {
			
			Scanner scanner = accumuloConnector.createScanner(tempKMeansTableNamespace, new Authorizations());

			IteratorSetting iter = ClusteringUtils.createScanIterator("GeoSearch Filter", clusterAssignmentRowId, null, null, null, false);
			scanner.addScanIterator(iter);

			// retrieve list of colfam (centroid id) values
			Set<String> centroidList = new HashSet<String>();
			for(final Entry<Key,Value> entry : scanner)
			{
				centroidList.add(entry.getKey().getColumnFamily().toString());
			}
			scanner.clearScanIterators();		

			// set up map/reduce job
			AuthenticationToken authToken = new PasswordToken(password.getBytes());
			Job job = new Job(new Configuration(), "GeoSearch, Polygon Generation");	

			job.getConfiguration().set("zookeeper.instanceName", instanceName);
			job.getConfiguration().set("zookeeper.zooservers", zooservers);
			job.getConfiguration().set("accumulo.userName", user);
			job.getConfiguration().set("accumulo.password", password);

			job.getConfiguration().set("outputRowId", outputRowId);

			// extends wait time to one hour (default: 600 seconds)
			long milliSeconds = 1000*60*60;
			job.getConfiguration().setLong("mapred.task.timeout", milliSeconds);

			job.setJarByClass(PolygonGenerationDriver.class);

			// the actual classes used in this job
			job.setMapperClass(PolygonGenerationMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(PolygonGenerationReducer.class);
			job.setNumReduceTasks(centroidList.size());

			job.setInputFormatClass(AccumuloInputFormat.class);
			job.setOutputFormatClass(AccumuloOutputFormat.class);

			// just process the input row
			ArrayList<Range> ranges = new ArrayList<Range>();
			ranges.add(new Range(clusterAssignmentRowId));				
			AccumuloInputFormat.setRanges(job, ranges);

			AccumuloInputFormat.setConnectorInfo(job, user, authToken);
			AccumuloInputFormat.setInputTableName(job, tempKMeansTableNamespace);
			AccumuloInputFormat.setScanAuthorizations(job, null);
			AccumuloInputFormat.setZooKeeperInstance(job, instanceName, zooservers);

			// set up AccumuloOutputFormat
			AccumuloOutputFormat.setConnectorInfo(job, user, authToken);
			AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zooservers);
			AccumuloOutputFormat.setDefaultTableName(job, tempKMeansTableNamespace);
			AccumuloOutputFormat.setCreateTables(job, true);

			// add all the dependency jars to the distributed cache for all map/reduce tasks
			// all jars must be on hdfs at the specified directory prior to running job
			FileSystem fs = FileSystem.get(job.getConfiguration());
			Path dcache = new Path("/data/cache/lib");
			try {
				FileStatus[] jars = fs.globStatus(new Path( dcache.toString() + "/*.jar"));
				for (int i=0; i< jars.length; i++) {
					Path path = jars[i].getPath();
					if (fs.exists(path) && jars[i].isFile()) {
						DistributedCache.addFileToClassPath(new Path(dcache.toString() + "/" + path.getName()), job.getConfiguration(), fs);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			job.waitForCompletion(true);

			// retrieve all the centroid polygons and form a multipolygon for final output
			iter = ClusteringUtils.createScanIterator("GeoSearch Filter", outputRowId, "SUMMARY", null, null, false);
			scanner.addScanIterator(iter);

			// retrieve list of polygon strings, in WKB format
			WKBReader wkbReader = new WKBReader();
			List<Polygon> polygons = new ArrayList<Polygon>();
			for(final Entry<Key,Value> entry : scanner)
			{
				Geometry geometry = wkbReader.read(entry.getValue().get());
				if(geometry instanceof Polygon)
				{
					polygons.add((Polygon) geometry);
				}
			}
			scanner.clearScanIterators();	
			scanner.close();

			com.vividsolutions.jts.geom.Polygon[] sortedPolygonArray = new com.vividsolutions.jts.geom.Polygon[polygons.size()];
			int polygonCounter = 0;
			for(Polygon polygon : polygons)
			{
				sortedPolygonArray[polygonCounter] = polygon;
				polygonCounter++;
			}

			// in the end, there can be only one... (output object)
			MultiPolygon multiPolygon = new MultiPolygon(sortedPolygonArray, new GeometryFactory());
			
			// index multipolygon result back to GeoWave
			DataStore dataStore = new AccumuloDataStore(new BasicAccumuloOperations(accumuloConnector, outputTableNamespace));		
			Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();		 
			
			// build a multipolygon feature type
			SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
	        builder.setName("MultiPolygon");
	        builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference system

	        // add attributes in order
	        builder.add("geom", MultiPolygon.class);
	        builder.add("name", String.class);
	        
	        // build the type
	        final SimpleFeatureType multiPolygonType = builder.buildFeatureType();

	    	WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(multiPolygonType);
			SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(multiPolygonType);
			
			// write the multipolygon to GeoWave
			featureBuilder.add(multiPolygon);
			featureBuilder.add(outputRowId); // name attribute
			SimpleFeature feature = featureBuilder.buildFeature(null);
			dataStore.ingest(adapter, index, feature);		
		}
		catch (TableNotFoundException | IOException | ParseException | ClassNotFoundException | InterruptedException | AccumuloSecurityException e1) {
			e1.printStackTrace();
		}
		
		return outputRowId;
	}
}
