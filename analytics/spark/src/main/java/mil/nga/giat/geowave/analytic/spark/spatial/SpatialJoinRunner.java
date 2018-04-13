package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.GeoWaveSparkConf;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class SpatialJoinRunner implements
		Serializable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialJoinRunner.class);

	// Options provided by user to run join
	private SparkSession session = null;
	private SparkContext sc = null;
	private String appName = "SpatialJoinRunner";
	private String master = "yarn";
	private String host = "localhost";
	private Integer partCount = -1;
	private DataStorePluginOptions leftStore = null;
	private ByteArrayId leftAdapterId = null;
	private ByteArrayId outLeftAdapterId = null;
	private DataStorePluginOptions rightStore = null;
	private ByteArrayId rightAdapterId = null;
	private ByteArrayId outRightAdapterId = null;

	private DataStorePluginOptions outputStore = null;
	private GeomFunction predicate = null;
	private NumericIndexStrategy indexStrategy = null;
	// Variables loaded during runner. This can be updated to something cleaner
	// like GeoWaveRDD in future
	// to support different situations (indexed vs non indexed etc..) but keep
	// it hidden in implementation details
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD = null;
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD = null;

	// TODO: Join strategy could be supplied as variable or determined
	// automatically from index store (would require associating index and join
	// strategy)
	// for now will just use TieredSpatialJoin as that is the only one we have
	// implemented.
	private JoinStrategy joinStrategy = new TieredSpatialJoin();

	public SpatialJoinRunner() {}

	public SpatialJoinRunner(
			SparkSession session ) {
		this.session = session;
	}

	public void run()
			throws InterruptedException,
			ExecutionException,
			IOException {
		// Init context
		initContext();
		// Load RDDs
		loadDatasets();
		// Verify CRS match/transform possible
		verifyCRS();
		// Run join
		joinStrategy.join(
				session,
				leftRDD,
				rightRDD,
				predicate,
				indexStrategy);

		writeResultsToNewAdapter();
	}

	private void writeResultsToNewAdapter()
			throws IOException {
		if (outputStore != null) {
			ByteArrayId leftJoinId = outLeftAdapterId;
			if (leftJoinId == null) {
				leftJoinId = createDefaultAdapterName(
						leftAdapterId,
						leftStore);
			}
			FeatureDataAdapter newLeftAdapter = FeatureDataUtils.cloneFeatureDataAdapter(
					leftStore,
					leftAdapterId,
					leftJoinId);
			PrimaryIndex[] leftIndices = leftStore.createAdapterIndexMappingStore().getIndicesForAdapter(
					leftAdapterId).getIndices(
					leftStore.createIndexStore());
			newLeftAdapter.init(leftIndices);

			ByteArrayId rightJoinId = outRightAdapterId;
			if (rightJoinId == null) {
				rightJoinId = createDefaultAdapterName(
						rightAdapterId,
						rightStore);
			}
			FeatureDataAdapter newRightAdapter = FeatureDataUtils.cloneFeatureDataAdapter(
					rightStore,
					rightAdapterId,
					rightJoinId);
			PrimaryIndex[] rightIndices = rightStore.createAdapterIndexMappingStore().getIndicesForAdapter(
					rightAdapterId).getIndices(
					rightStore.createIndexStore());
			newRightAdapter.init(rightIndices);
			// Write each feature set to new adapter and store using original
			// indexing methods.
			GeoWaveRDD.writeFeaturesToGeoWave(
					sc,
					leftIndices,
					outputStore,
					newLeftAdapter,
					this.getLeftResults());
			GeoWaveRDD.writeFeaturesToGeoWave(
					sc,
					rightIndices,
					outputStore,
					newRightAdapter,
					this.getRightResults());
		}
	}

	private ByteArrayId createDefaultAdapterName(
			ByteArrayId adapterId,
			DataStorePluginOptions storeOptions ) {
		String defaultAdapterName = adapterId + "_joined";
		ByteArrayId defaultAdapterId = new ByteArrayId(
				defaultAdapterName);
		AdapterStore adapterStore = storeOptions.createAdapterStore();
		if (!adapterStore.adapterExists(defaultAdapterId)) {
			return defaultAdapterId;
		}
		Integer iSuffix = 0;
		String uniNum = "_" + String.format(
				"%02d",
				iSuffix);
		defaultAdapterId = new ByteArrayId(
				defaultAdapterName + uniNum);
		while (adapterStore.adapterExists(defaultAdapterId)) {
			// Should be _00 _01 etc
			iSuffix += 1;
			uniNum = "_" + String.format(
					"%02d",
					iSuffix);
			defaultAdapterId = new ByteArrayId(
					defaultAdapterName + uniNum);
		}
		return defaultAdapterId;
	}

	private void initContext() {
		if (session == null) {
			String jar = "";
			try {
				jar = SpatialJoinRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			}
			catch (final URISyntaxException e) {
				LOGGER.error(
						"Unable to set jar location in spark configuration",
						e);
			}
			SparkConf addonOptions = new SparkConf();
			addonOptions = addonOptions.setAppName(
					appName).setMaster(
					master).set(
					"spark.driver.host",
					host).set(
					"spark.jars",
					jar);

			// Since default parallelism is normally set by spark-defaults only
			// set this to config if supplied by user
			if (partCount != -1) {
				addonOptions = addonOptions.set(
						"spark.default.parallelism",
						partCount.toString());
			}
			session = GeoWaveSparkConf.createDefaultSession(addonOptions);
		}
		sc = session.sparkContext();
	}

	private void loadDatasets()
			throws IOException {
		if (leftStore != null) {
			if (leftRDD == null) {
				QueryOptions leftOptions = null;

				// Load left Adapter
				if (leftAdapterId == null) {
					// If no adapterId provided by user grab first adapterId
					// available.
					leftAdapterId = FeatureDataUtils.getFeatureAdapterIds(
							leftStore).get(
							0);
				}

				leftOptions = new QueryOptions(
						leftStore.createAdapterStore().getAdapter(
								leftAdapterId));

				leftRDD = GeoWaveRDD.rddForSimpleFeatures(
						sc,
						leftStore,
						null,
						leftOptions);
			}

		}

		if (rightStore != null) {
			QueryOptions rightOptions = null;

			if (rightAdapterId == null) {
				// If no adapterId provided by user grab first adapterId
				// available.
				rightAdapterId = FeatureDataUtils.getFeatureAdapterIds(
						rightStore).get(
						0);
			}

			rightOptions = new QueryOptions(
					rightStore.createAdapterStore().getAdapter(
							rightAdapterId));

			if (rightRDD == null) {
				rightRDD = GeoWaveRDD.rddForSimpleFeatures(
						sc,
						rightStore,
						null,
						rightOptions);

			}

		}

	}

	private void verifyCRS() {
		// TODO: Verify that both stores have matching CRS or that one CRS can
		// be transformed into the other
	}

	// Accessors and Mutators
	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getLeftResults() {
		return this.joinStrategy.getLeftResults();
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getRightResults() {
		return this.joinStrategy.getRightResults();
	}

	public DataStorePluginOptions getLeftStore() {
		return leftStore;
	}

	public void setLeftStore(
			DataStorePluginOptions leftStore ) {
		this.leftStore = leftStore;
	}

	public ByteArrayId getLeftAdapterId() {
		return leftAdapterId;
	}

	public void setLeftAdapterId(
			ByteArrayId leftAdapterId ) {
		this.leftAdapterId = leftAdapterId;
	}

	public DataStorePluginOptions getRightStore() {
		return rightStore;
	}

	public void setRightStore(
			DataStorePluginOptions rightStore ) {
		this.rightStore = rightStore;
	}

	public ByteArrayId getRightAdapterId() {
		return rightAdapterId;
	}

	public void setRightAdapterId(
			ByteArrayId rightAdapterId ) {
		this.rightAdapterId = rightAdapterId;
	}

	public DataStorePluginOptions getOutputStore() {
		return outputStore;
	}

	public void setOutputStore(
			DataStorePluginOptions outputStore ) {
		this.outputStore = outputStore;
	}

	public GeomFunction getPredicate() {
		return predicate;
	}

	public void setPredicate(
			GeomFunction predicate ) {
		this.predicate = predicate;
	}

	public NumericIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public void setIndexStrategy(
			NumericIndexStrategy indexStrategy ) {
		this.indexStrategy = indexStrategy;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(
			String appName ) {
		this.appName = appName;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(
			String master ) {
		this.master = master;
	}

	public String getHost() {
		return host;
	}

	public void setHost(
			String host ) {
		this.host = host;
	}

	public Integer getPartCount() {
		return partCount;
	}

	public void setPartCount(
			Integer partCount ) {
		this.partCount = partCount;
	}

	public void setSession(
			SparkSession session ) {
		this.session = session;
	}

	public ByteArrayId getOutputLeftAdapterId() {
		return outLeftAdapterId;
	}

	public void setOutputLeftAdapterId(
			ByteArrayId outLeftAdapterId ) {
		this.outLeftAdapterId = outLeftAdapterId;
	}

	public ByteArrayId getOutputRightAdapterId() {
		return outRightAdapterId;
	}

	public void setOutputRightAdapterId(
			ByteArrayId outRightAdapterId ) {
		this.outRightAdapterId = outRightAdapterId;
	}

}