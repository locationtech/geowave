package mil.nga.giat.geowave.analytics.mapreduce.geosearch.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.GeoWaveInputFormat;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;

public class KMeansDriver
{
	// configuration for Accumulo
	private String instanceName;
	private String zooservers;
	private String user;
	private String password;
	private Instance zookeeperInstance;
	private Connector accumuloConnector;
	
	private final String dataTableNamespace;
	private String tempKMeansTableName;
	private String runId;

	private final double convergenceTol = 10e-3;

	/**
	 * @param instanceName String Name of Accumulo instance
	 * @param zooservers String CSV of zookeeper addresses
	 * @param user String User for Accumulo instance
	 * @param password String Password for access
	 * @param dataTableNamespace String GeoWave namespace for input data table, only run data shall be stored in this table
	 * @param tempKMeansTableName String Namespace for temporary Accumulo table to hold inter-run data 
	 */
	public KMeansDriver(
			String instanceName,
			String zooservers,
			String user,
			String password,
			final String dataTableNamespace,
			final String tempKMeansTableName) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableName = tempKMeansTableName;

		connectToAccumulo();
	}

	public KMeansDriver(
			String instanceName,
			String zooservers,
			String user,
			String password,
			final String tempKMeansTableName,
			final String dataTableNamespace,
			final Connector conn ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableName = tempKMeansTableName;
		
		accumuloConnector = conn;
	}

	/**
	 * To connect to Accumulo, one must first contact Zookeeper because it is
	 * the service that holds all of the configurations and settings for
	 * Accumulo
	 */
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
	}

	/**
	 * Method runs the K-Means clustering process until convergence
	 * or until maximum allowable iteration count has been reached
	 */
	public Integer runKMeans(
			final String runId,
			final String dataTypeId,
			final int numClusters )
			throws Exception {
		this.runId = runId;

		System.out.println("K-Means Driver Running...");
		
		Polygon polygon = ClusteringUtils.generateWorldPolygon();

		final AuthenticationToken authToken = new PasswordToken(
				password.getBytes());

		Integer iterCounter = 0;

		/*
		 * Initialize the centroids, writes centroid data to accumulo table
		 */
		initializeCentroids(
				dataTypeId,
				numClusters,
				polygon);
		iterCounter++;

		/*
		 * Set up job to run for iteration 2 - n, until convergence
		 */
		boolean converged = false;
		while (!converged && (iterCounter <= 50)) {
			final Job job = new Job(
					new Configuration(),
					"GeoSearch, K-Means, iteration: " + iterCounter);

			job.getConfiguration().set(
					"zookeeper.instanceName",
					instanceName);
			job.getConfiguration().set(
					"zookeeper.zooservers",
					zooservers);
			job.getConfiguration().set(
					"accumulo.userName",
					user);
			job.getConfiguration().set(
					"accumulo.password",
					password);
			job.getConfiguration().set(
					"data.table",
					dataTableNamespace);
			job.getConfiguration().set(
					"run.id",
					runId);
			job.getConfiguration().set(
					"kmeans.table",
					tempKMeansTableName);
			job.getConfiguration().set(
					"iteration.number",
					iterCounter.toString());
			job.getConfiguration().set("dataTypeId", dataTypeId);

			job.setJarByClass(KMeansDriver.class);

			// the actual classes used in this job
			job.setMapperClass(KMeansMapper.Map.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(KMeansReducer.Reduce.class);
			job.setNumReduceTasks(numClusters);

			job.setInputFormatClass(GeoWaveInputFormat.class);
			job.setOutputFormatClass(AccumuloOutputFormat.class);

			InputFormatBase.setConnectorInfo(
					job,
					user,
					authToken);
			InputFormatBase.setInputTableName(
					job,
					dataTableNamespace);
			InputFormatBase.setScanAuthorizations(
					job,
					null);
			InputFormatBase.setZooKeeperInstance(
					job,
					instanceName,
					zooservers);
			// split ranges for polygon query into individual map tasks
			GeoWaveInputFormat.setRangesForPolygon(
					job,
					polygon);
			
			// set up AccumuloOutputFormat
			AccumuloOutputFormat.setConnectorInfo(job, user, authToken);
			AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zooservers);
			AccumuloOutputFormat.setDefaultTableName(job, tempKMeansTableName);
			AccumuloOutputFormat.setCreateTables(job, true);

			// add all the dependency jars to the distributed cache for all
			// map/reduce tasks
			// all jars must be on hdfs at the specified directory prior to
			// running job
			final FileSystem fs = FileSystem.get(job.getConfiguration());
			final Path dcache = new Path(
					"/data/cache/lib");
			try {
				final FileStatus[] jars = fs.globStatus(new Path(
						dcache.toString() + "/*.jar"));
				for (int i = 0; i < jars.length; i++) {
					final Path path = jars[i].getPath();
					if (fs.exists(path) && jars[i].isFile()) {
						DistributedCache.addFileToClassPath(
								new Path(
										dcache.toString() + "/" + path.getName()),
								job.getConfiguration(),
								fs);
					}
				}
			}
			catch (final IOException e) {
				e.printStackTrace();
			}

			job.waitForCompletion(true);

			// check for convergence
			converged = checkForConvergence(iterCounter);

			iterCounter++;
		}

		return iterCounter - 1;
	}

	/**
	 * Centroid initialization via random data assignments
	 */
	private List<DataPoint> initializeCentroids(
			final String dataTypeId,
			final int numClusters,
			final Polygon polygon )
			throws SchemaException {
		System.out.println("Initializing centroids, num centroids: " + numClusters);

		List<DataPoint> points = new ArrayList<DataPoint>();

		// spatial index definition and query stuff
		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		// stuff to build SimpleFeature objects
		final SimpleFeatureType type = ClusteringUtils.createSimpleFeatureType(dataTypeId);

		// stuff to interact with GeoWave
		final WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
				type);
		final DataStore inputDataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumuloConnector,
						dataTableNamespace));
		
		int numPts = ClusteringUtils.getPointCount(inputDataStore, adapter, index, polygon);

		final Random random = new Random();
		// pick out numPts of random numbers for picking centroid points
		final List<Integer> randInts = new ArrayList<Integer>();
		if (numPts < (numClusters * 10)) {

			// small sample size, just pick the first points for centroids
			for (int ii = 0; ii < numPts; ii++) {
				randInts.add(ii);
			}
		}
		else {
			// large sample size, pick random points for centroids
			for (int ii = 0; ii < numPts; ii++) {
				Integer randInt = random.nextInt(numPts);

				// make sure it's not a repeated number
				int breaker = 0;
				while (randInts.contains(randInt) && (breaker < 10)) {
					randInt = random.nextInt(numPts);
					breaker++;
				}
				randInts.add(randInt);
			}

			// sort the picks in ascending order
			Collections.sort(randInts);
		}

		// extract points from GeoWave
		points = ClusteringUtils.getSpecifiedPoints(
				inputDataStore,
				adapter,
				index,
				polygon,
				randInts);
		
		// write centroids to a custom temporary Accumulo table on the GeoWave instance
		try {
			if(!accumuloConnector.tableOperations().exists(tempKMeansTableName))
					accumuloConnector.tableOperations().create(tempKMeansTableName);
			
			BatchWriter writer = accumuloConnector.createBatchWriter(tempKMeansTableName, new BatchWriterConfig());
			
			// write initial clusters to table
			Integer centroidCount= 0;
			for(DataPoint centroid : points)
			{		
				/*
				 *  writes to the centroid keeper row
				 *  
				 *  run_id | iter# | centroid_id | pt_Str (x,y)
				 */
				Coordinate coord = new Coordinate(centroid.x, centroid.y);
				Mutation mutation = new Mutation(runId);
				mutation.put(new Text("0"), new Text(centroidCount.toString()), new Value(coord.toString().getBytes()));

				writer.addMutation(mutation);
			}
					
			writer.close();
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableExistsException e) {
			e.printStackTrace();
		}

		return points;
	}
	
	/*
	 * null field will result in all possible entries in that field
	 */
	private IteratorSetting createScanIterator(String iterName, String rowRegex, String colfRegex, String colqRegex, String valRegex, boolean orFields)
	{
		IteratorSetting iter = new IteratorSetting(15, iterName, RegExFilter.class);
		RegExFilter.setRegexs(iter, rowRegex, colfRegex, colqRegex, valRegex, orFields);	
		
		return iter;
	}

	/*
	 * convergence metric is the average centroid movement between the previous
	 * and current iteration
	 */
	private boolean checkForConvergence(Integer iterCount)
	{
		Scanner scanner;
		try {
			scanner = accumuloConnector.createScanner(tempKMeansTableName, new Authorizations());			
			
			// retrieve centroid information from previous run
			Integer previousIter = iterCount - 1;
			scanner.addScanIterator(createScanIterator("GeoSearch filter", runId, previousIter.toString(), null, null, false));
			HashMap<Integer, DataPoint> idToCentroidMap = new HashMap<Integer,DataPoint>();
			for(final Entry<Key,Value> entry : scanner)
			{
				// entries should be centroid locations for previous run
				String ptStr = entry.getValue().toString();
				ptStr = ptStr.substring(1, ptStr.length() -1);
				String[] splits = ptStr.split(",");
				
				int id = Integer.parseInt(entry.getKey().getColumnQualifier().toString());
				
				DataPoint dp = new DataPoint(id, Double.parseDouble(splits[0]), Double.parseDouble(splits[1]), id, true);
				idToCentroidMap.put(id, dp);
			}
			scanner.clearScanIterators();

			// retrieve latest centroid information
			scanner.addScanIterator(createScanIterator("GeoSearch filter", runId, iterCount.toString(), null, null, false));
			double diff = 0.0;
			for(final Entry<Key,Value> entry : scanner)
			{
				// entries should be centroid locations for latest run
				String ptStr = entry.getValue().toString();
				ptStr = ptStr.substring(1, ptStr.length() -1);
				String[] splits = ptStr.split(",");
				
				System.out.println("iteration: " + entry.getKey().getColumnFamily().toString() + ", centroid: " + entry.getKey().getColumnQualifier().toString() + ", (" + entry.getValue().toString() + ")");
				
				int id = Integer.parseInt(entry.getKey().getColumnQualifier().toString());
				
				DataPoint dp = new DataPoint(id, Double.parseDouble(splits[0]), Double.parseDouble(splits[1]), id, true);
				if(idToCentroidMap.get(id) != null)
				{
					diff += idToCentroidMap.get(id).calculateDistance(dp);
				}
				else
				{
					System.err.println("current run (" + iterCount + ") centroid does not exist in previous run centroid list, this is not good!");
				}			
			}
			scanner.clearScanIterators();
			
			scanner.close();
			
			diff /= (double) idToCentroidMap.size();
			
			System.out.println("run: " + iterCount + ", avg centroid movement: " + diff);
			
			if(diff < convergenceTol)	return true;
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		
		return false;
	}

}
