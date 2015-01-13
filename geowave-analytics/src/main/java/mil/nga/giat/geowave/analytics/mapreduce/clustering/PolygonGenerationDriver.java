package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

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
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private final String tempKMeansTableName;

	private final String outputTableNamespace;

	final Logger log = LoggerFactory.getLogger(PolygonGenerationDriver.class);

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
	 * @param tempKMeansTableName
	 *            String Table for storing intra-processing information
	 */
	public PolygonGenerationDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String tempKMeansTableName,
			final String outputTableNamespace ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.tempKMeansTableName = tempKMeansTableName;
		this.outputTableNamespace = outputTableNamespace;

		connectToAccumulo();
	}

	public PolygonGenerationDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String tempKMeansTableName,
			final String outputTableNamespace,
			final Connector accumuloConnector ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.tempKMeansTableName = tempKMeansTableName;
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
		catch (final AccumuloException | AccumuloSecurityException e) {
			log.error(
					"Output writer mapper: " + e.getMessage(),
					e);
		}
	}

	public String generatePolygons(
			final String clusterAssignmentRowId ) {
		log.info("Starting polygon generation...");

		final String outputRowId = "Polygons_" + UUID.randomUUID().toString();

		try {

			final Scanner scanner = accumuloConnector.createScanner(
					tempKMeansTableName,
					new Authorizations());

			IteratorSetting iter = ClusteringUtils.createScanIterator(
					"GeoSearch Filter",
					clusterAssignmentRowId,
					null,
					null,
					null,
					false);
			scanner.addScanIterator(iter);

			// retrieve list of colfam (centroid id) values
			final Set<String> centroidList = new HashSet<String>();
			for (final Entry<Key, Value> entry : scanner) {
				centroidList.add(entry.getKey().getColumnFamily().toString());
			}
			scanner.clearScanIterators();

			// set up map/reduce job
			final AuthenticationToken authToken = new PasswordToken(
					password.getBytes());
			final Job job = new Job(
					new Configuration(),
					"GeoSearch, Polygon Generation");

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
					"outputRowId",
					outputRowId);

			// extends wait time to one hour (default: 600 seconds)
			final long milliSeconds = 1000 * 60 * 60;
			job.getConfiguration().setLong(
					"mapred.task.timeout",
					milliSeconds);

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
			final ArrayList<Range> ranges = new ArrayList<Range>();
			ranges.add(new Range(
					clusterAssignmentRowId));
			InputFormatBase.setRanges(
					job,
					ranges);

			InputFormatBase.setConnectorInfo(
					job,
					user,
					authToken);
			InputFormatBase.setInputTableName(
					job,
					tempKMeansTableName);
			InputFormatBase.setScanAuthorizations(
					job,
					null);
			InputFormatBase.setZooKeeperInstance(
					job,
					instanceName,
					zooservers);

			// set up AccumuloOutputFormat
			AccumuloOutputFormat.setConnectorInfo(
					job,
					user,
					authToken);
			AccumuloOutputFormat.setZooKeeperInstance(
					job,
					instanceName,
					zooservers);
			AccumuloOutputFormat.setDefaultTableName(
					job,
					tempKMeansTableName);
			AccumuloOutputFormat.setCreateTables(
					job,
					true);

			job.waitForCompletion(true);

			// retrieve all the centroid polygons and form a multipolygon for
			// final output
			iter = ClusteringUtils.createScanIterator(
					"GeoSearch Filter",
					outputRowId,
					"SUMMARY",
					null,
					null,
					false);
			scanner.addScanIterator(iter);

			// retrieve list of polygon strings, in WKB format
			final WKBReader wkbReader = new WKBReader();
			// WKTWriter wktWriter = new WKTWriter();
			final List<Polygon> polygons = new ArrayList<Polygon>();
			for (final Entry<Key, Value> entry : scanner) {
				final Geometry geometry = wkbReader.read(entry.getValue().get());
				if (geometry instanceof Polygon) {
					// Polygon polygon = (Polygon) geometry;
					// log.info(wktWriter.write(polygon));
					polygons.add((Polygon) geometry);
				}
			}
			scanner.clearScanIterators();
			scanner.close();

			if (polygons.size() == 0) {
				return null;
			}

			final com.vividsolutions.jts.geom.Polygon[] sortedPolygonArray = new com.vividsolutions.jts.geom.Polygon[polygons.size()];
			int polygonCounter = 0;
			for (final Polygon polygon : polygons) {
				sortedPolygonArray[polygonCounter] = polygon;
				polygonCounter++;
			}

			// in the end, there can be only one... (output object)
			final MultiPolygon multiPolygon = new MultiPolygon(
					sortedPolygonArray,
					new GeometryFactory());

			// index multipolygon result back to GeoWave
			final DataStore dataStore = new AccumuloDataStore(
					new BasicAccumuloOperations(
							accumuloConnector,
							outputTableNamespace));
			final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

			// build a multipolygon feature type
			final SimpleFeatureType multiPolygonType = ClusteringUtils.createMultiPolygonSimpleFeatureType("MultiPolygon");

			final WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
					multiPolygonType);
			final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
					multiPolygonType);

			// write the multipolygon to GeoWave
			featureBuilder.add(multiPolygon);
			featureBuilder.add(outputRowId); // name attribute
			final SimpleFeature feature = featureBuilder.buildFeature(null);
			dataStore.ingest(
					adapter,
					index,
					feature);
		}
		catch (TableNotFoundException | IOException | ParseException | ClassNotFoundException | InterruptedException | AccumuloSecurityException e) {
			log.error(
					"Polygon generation driver: " + e.getMessage(),
					e);
		}

		return outputRowId;
	}
}
