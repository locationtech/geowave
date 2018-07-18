package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.analytic.spark.GeoWaveIndexedRDD;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDDLoader;
import mil.nga.giat.geowave.analytic.spark.GeoWaveSparkConf;
import mil.nga.giat.geowave.analytic.spark.RDDOptions;
import mil.nga.giat.geowave.analytic.spark.RDDUtils;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

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
	private boolean negativeTest = false;

	private DataStorePluginOptions outputStore = null;
	private GeomFunction predicate = null;
	private NumericIndexStrategy indexStrategy = null;
	// Variables loaded during runner. This can be updated to something cleaner
	// like GeoWaveRDD in future
	// to support different situations (indexed vs non indexed etc..) but keep
	// it hidden in implementation details
	private GeoWaveIndexedRDD leftRDD = null;
	private GeoWaveIndexedRDD rightRDD = null;

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

		joinStrategy.getJoinOptions().setNegativePredicate(
				negativeTest);
		joinStrategy.join(
				session,
				leftRDD,
				rightRDD,
				predicate);

		writeResultsToNewAdapter();
	}

	private PrimaryIndex[] getIndicesForAdapter(
			DataStorePluginOptions storeOptions,
			ByteArrayId adapterId ) {
		return storeOptions.createAdapterIndexMappingStore().getIndicesForAdapter(
				adapterId).getIndices(
				storeOptions.createIndexStore());
	}

	private FeatureDataAdapter createOutputAdapter(
			DataStorePluginOptions originalOptions,
			ByteArrayId originalId,
			PrimaryIndex[] indices,
			ByteArrayId outputAdapterId ) {

		if (outputAdapterId == null) {
			outputAdapterId = createDefaultAdapterName(
					originalId,
					originalOptions);
		}
		FeatureDataAdapter newAdapter = FeatureDataUtils.cloneFeatureDataAdapter(
				originalOptions,
				originalId,
				outputAdapterId);
		newAdapter.init(indices);
		return newAdapter;
	}

	private void writeResultsToNewAdapter()
			throws IOException {
		if (outputStore != null) {

			PrimaryIndex[] leftIndices = this.getIndicesForAdapter(
					leftStore,
					leftAdapterId);
			FeatureDataAdapter newLeftAdapter = this.createOutputAdapter(
					leftStore,
					leftAdapterId,
					leftIndices,
					outLeftAdapterId);

			PrimaryIndex[] rightIndices = this.getIndicesForAdapter(
					rightStore,
					rightAdapterId);
			FeatureDataAdapter newRightAdapter = this.createOutputAdapter(
					rightStore,
					rightAdapterId,
					rightIndices,
					outRightAdapterId);
			// Write each feature set to new adapter and store using original
			// indexing methods.
			RDDUtils.writeRDDToGeoWave(
					sc,
					leftIndices,
					outputStore,
					newLeftAdapter,
					this.getLeftResults());
			RDDUtils.writeRDDToGeoWave(
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
			SparkConf addonOptions = GeoWaveSparkConf.getDefaultConfig();
			addonOptions = addonOptions.setAppName(
					appName).setMaster(
					master).set(
					"spark.jars",
					jar);

			if (!Objects.equals(
					master,
					"yarn")) {
				addonOptions = addonOptions.set(
						"spark.driver.host",
						host);
			}

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

	private GeoWaveIndexedRDD createRDDFromOptions(
			DataStorePluginOptions storeOptions,
			ByteArrayId adapterId )
			throws IOException {
		QueryOptions adapterOptions;

		// If no adapterId provided by user grab first adapterId
		// available.
		if (adapterId == null) {
			List<ByteArrayId> byteIds = FeatureDataUtils.getFeatureAdapterIds(storeOptions);
			if (!byteIds.isEmpty()) {
				adapterId = byteIds.get(0);
			}
			else {
				LOGGER.error("No valid adapter found in store to perform join.");
				return null;
			}
		}

		adapterOptions = new QueryOptions(
				storeOptions.createAdapterStore().getAdapter(
						adapterId));

		RDDOptions rddOpts = new RDDOptions();
		rddOpts.setQueryOptions(adapterOptions);
		rddOpts.setMinSplits(this.partCount);
		rddOpts.setMaxSplits(this.partCount);

		NumericIndexStrategy rddStrategy = null;
		// Did the user provide a strategy for join?
		if (this.indexStrategy == null) {
			PrimaryIndex[] leftIndices = this.getIndicesForAdapter(
					storeOptions,
					adapterId);
			if (leftIndices.length > 0) {
				rddStrategy = leftIndices[0].getIndexStrategy();
			}

		}
		else {
			rddStrategy = this.indexStrategy;
		}

		return GeoWaveRDDLoader.loadIndexedRDD(
				sc,
				storeOptions,
				rddOpts,
				rddStrategy);
	}

	private void loadDatasets()
			throws IOException {
		if (leftStore != null) {
			if (leftRDD == null) {
				leftRDD = this.createRDDFromOptions(
						leftStore,
						leftAdapterId);
			}
		}

		if (rightStore != null) {
			if (rightRDD == null) {
				rightRDD = this.createRDDFromOptions(
						rightStore,
						rightAdapterId);
			}

		}

	}

	private void verifyCRS() {
		// TODO: Verify that both stores have matching CRS or that one CRS can
		// be transformed into the other
	}

	// Accessors and Mutators
	public GeoWaveRDD getLeftResults() {
		return this.joinStrategy.getLeftResults();
	}

	public GeoWaveRDD getRightResults() {
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

	public void setLeftRDD(
			GeoWaveIndexedRDD leftRDD ) {
		this.leftRDD = leftRDD;
	}

	public void setRightRDD(
			GeoWaveIndexedRDD rightRDD ) {
		this.rightRDD = rightRDD;
	}

	public boolean isNegativeTest() {
		return negativeTest;
	}

	public void setNegativeTest(
			boolean negativeTest ) {
		this.negativeTest = negativeTest;
	}

}