package mil.nga.giat.geowave.analytic.javaspark;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.store.query.ScaledTemporalRange;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class KMeansRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KMeansRunner.class);

	private String appName = "KMeansRunner";
	private String master = "local";
	private String host = "localhost";

	private JavaSparkContext jsc = null;
	private DataStorePluginOptions inputDataStore = null;
	private JavaRDD<Vector> centroidVectors;
	private KMeansModel outputModel;

	private int numClusters = 8;
	private int numIterations = 20;
	private double epsilon = -1.0;
	private Envelope bbox = null;
	private String adapterId = null;
	private String timeField = null;
	private ScaledTemporalRange scaledTimeRange = null;

	public KMeansRunner() {}

	private void initContext() {
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName(appName);
		sparkConf.setMaster(master);

		// TODO: other context config settings

		jsc = new JavaSparkContext(
				sparkConf);
	}

	public void closeContext() {
		if (jsc != null) {
			jsc.close();
			jsc = null;
		}
	}

	public void run()
			throws IOException {
		initContext();

		// Validate inputs
		if (inputDataStore == null) {
			LOGGER.error("You must supply an input datastore!");
			throw new IOException(
					"You must supply an input datastore!");
		}

		QueryOptions queryOptions = null;
		if (adapterId != null) {
			// Retrieve the adapters
			CloseableIterator<DataAdapter<?>> adapterIt = inputDataStore.createAdapterStore().getAdapters();
			DataAdapter adapterForQuery = null;

			while (adapterIt.hasNext()) {
				DataAdapter adapter = adapterIt.next();
				String adapterName = StringUtils.stringFromBinary(adapter.getAdapterId().getBytes());

				if (adapterName.equals(adapterId)) {
					adapterForQuery = adapter;
					queryOptions = new QueryOptions(
							adapterForQuery);
					break;
				}
			}
		}

		DistributableQuery query = null;
		if (bbox != null) {
			Geometry geom = new GeometryFactory().toGeometry(bbox);
			query = new SpatialQuery(
					geom);
		}

		// Load RDD from datastore
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaPairRdd = GeoWaveRDD.rddForSimpleFeatures(
				jsc.sc(),
				inputDataStore,
				query,
				queryOptions);

		// Retrieve the input centroids
		centroidVectors = GeoWaveRDD.rddFeatureVectors(
				javaPairRdd,
				timeField,
				scaledTimeRange);
		centroidVectors.cache();

		// Init the algorithm
		KMeans kmeans = new KMeans();
		kmeans.setInitializationMode("kmeans||");
		kmeans.setK(numClusters);
		kmeans.setMaxIterations(numIterations);

		if (epsilon > -1.0) {
			kmeans.setEpsilon(epsilon);
		}

		// Run KMeans
		outputModel = kmeans.run(centroidVectors.rdd());
	}

	public JavaRDD<Vector> getInputCentroids() {
		return centroidVectors;
	}

	public DataStorePluginOptions getInputDataStore() {
		return inputDataStore;
	}

	public void setInputDataStore(
			DataStorePluginOptions inputDataStore ) {
		this.inputDataStore = inputDataStore;
	}

	public void setNumClusters(
			int numClusters ) {
		this.numClusters = numClusters;
	}

	public void setNumIterations(
			int numIterations ) {
		this.numIterations = numIterations;
	}

	public void setEpsilon(
			Double epsilon ) {
		this.epsilon = epsilon;
	}

	public KMeansModel getOutputModel() {
		return outputModel;
	}

	public void setAppName(
			String appName ) {
		this.appName = appName;
	}

	public void setMaster(
			String master ) {
		this.master = master;
	}

	public void setHost(
			String host ) {
		this.host = host;
	}

	public void setBoundingBox(
			String bboxStr ) {
		try {
			// Expecting bbox in "LL-Lat LL-Lon UR-Lat UR-Lon" format
			StringTokenizer tok = new StringTokenizer(
					bboxStr,
					" ");
			String southStr = tok.nextToken();
			String westStr = tok.nextToken();
			String northStr = tok.nextToken();
			String eastStr = tok.nextToken();

			double west = Double.parseDouble(westStr);
			double east = Double.parseDouble(eastStr);
			double south = Double.parseDouble(southStr);
			double north = Double.parseDouble(northStr);

			this.bbox = new Envelope(
					west,
					east,
					south,
					north);
		}
		catch (Exception e) {
			LOGGER.error("Failed to parse bounding box from " + bboxStr);
			this.bbox = null;
		}
	}

	public void setAdapterId(
			String adapterId ) {
		this.adapterId = adapterId;
	}

	public void setTimeParams(
			String timeField,
			ScaledTemporalRange timeRange ) {
		this.timeField = timeField;
		this.scaledTimeRange = timeRange;
	}
}
