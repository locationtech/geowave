package mil.nga.giat.geowave.analytic.javaspark.kmeans;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitorResult;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.analytic.javaspark.GeoWaveRDD;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.ScaledTemporalRange;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
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
	private String cqlFilter = null;
	private String adapterId = null;
	private String timeField = null;
	private ScaledTemporalRange scaledTimeRange = null;
	private int minSplits = -1;
	private int maxSplits = -1;

	public KMeansRunner() {}

	private void initContext() {
		final SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName(appName);
		sparkConf.setMaster(master);
		try {
			sparkConf.setJars(new String[] {
				KMeansRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()
			});
		}
		catch (final URISyntaxException e) {
			LOGGER.error(
					"Unable to set jar location in spark configuration",
					e);
		}
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

		// Retrieve the feature adapters
		List<ByteArrayId> featureAdapterIds;

		// If provided, just use the one
		if (adapterId != null) {
			featureAdapterIds = new ArrayList<>();
			featureAdapterIds.add(new ByteArrayId(
					adapterId));
		}
		else { // otherwise, grab all the feature adapters
			featureAdapterIds = FeatureDataUtils.getFeatureAdapterIds(inputDataStore);
		}

		final QueryOptions queryOptions = new QueryOptions();
		queryOptions.setAdapter(featureAdapterIds);

		// This is required due to some funkiness in GeoWaveInputFormat
		final AdapterStore adapterStore = inputDataStore.createAdapterStore();
		queryOptions.getAdaptersArray(adapterStore);

		// Add a spatial filter if requested
		DistributableQuery query = null;
		try {
			if (cqlFilter != null) {
				Geometry bbox = null;
				ByteArrayId cqlAdapterId;
				if (adapterId == null) {
					cqlAdapterId = featureAdapterIds.get(0);
				}
				else {
					cqlAdapterId = new ByteArrayId(
							adapterId);
				}

				final DataAdapter adapter = adapterStore.getAdapter(cqlAdapterId);

				if (adapter instanceof FeatureDataAdapter) {
					final String geometryAttribute = ((FeatureDataAdapter) adapter)
							.getFeatureType()
							.getGeometryDescriptor()
							.getLocalName();
					Filter filter;
					filter = ECQL.toFilter(cqlFilter);

					final ExtractGeometryFilterVisitorResult geoAndCompareOpData = (ExtractGeometryFilterVisitorResult) filter
							.accept(
									new ExtractGeometryFilterVisitor(
											GeoWaveGTDataStore.DEFAULT_CRS,
											geometryAttribute),
									null);
					bbox = geoAndCompareOpData.getGeometry();
				}

				if ((bbox != null) && !bbox.equals(GeometryUtils.infinity())) {
					query = new SpatialQuery(
							bbox);
				}
			}
		}
		catch (final CQLException e) {
			LOGGER.error("Unable to parse CQL: " + cqlFilter);
		}

		// Load RDD from datastore
		final JavaPairRDD<GeoWaveInputKey, SimpleFeature> featureRdd = GeoWaveRDD.rddForSimpleFeatures(
				jsc.sc(),
				inputDataStore,
				query,
				queryOptions,
				minSplits,
				maxSplits);

		// Retrieve the input centroids
		centroidVectors = GeoWaveRDD.rddFeatureVectors(
				featureRdd,
				timeField,
				scaledTimeRange);
		centroidVectors.cache();

		// Init the algorithm
		final KMeans kmeans = new KMeans();
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
			final DataStorePluginOptions inputDataStore ) {
		this.inputDataStore = inputDataStore;
	}

	public void setNumClusters(
			final int numClusters ) {
		this.numClusters = numClusters;
	}

	public void setNumIterations(
			final int numIterations ) {
		this.numIterations = numIterations;
	}

	public void setEpsilon(
			final Double epsilon ) {
		this.epsilon = epsilon;
	}

	public KMeansModel getOutputModel() {
		return outputModel;
	}

	public void setAppName(
			final String appName ) {
		this.appName = appName;
	}

	public void setMaster(
			final String master ) {
		this.master = master;
	}

	public void setHost(
			final String host ) {
		this.host = host;
	}

	public void setCqlFilter(
			final String cqlFilter ) {
		this.cqlFilter = cqlFilter;
	}

	public void setAdapterId(
			final String adapterId ) {
		this.adapterId = adapterId;
	}

	public void setTimeParams(
			final String timeField,
			final ScaledTemporalRange timeRange ) {
		this.timeField = timeField;
		scaledTimeRange = timeRange;
	}

	public void setSplits(
			final int min,
			final int max ) {
		minSplits = min;
		maxSplits = max;
	}
}
