package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
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
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class SpatialJoinRunner implements
		Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

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

	private InternalAdapterStore leftInternalAdapterStore;
	private InternalAdapterStore rightInternalAdapterStore;

	private IndexStore leftIndexStore;
	private IndexStore rightIndexStore;

	// TODO: Join strategy could be supplied as variable or determined
	// automatically from index store (would require associating index and join
	// strategy)
	// for now will just use TieredSpatialJoin as that is the only one we have
	// implemented.
	private final JoinStrategy joinStrategy = new TieredSpatialJoin();

	public SpatialJoinRunner() {}

	public SpatialJoinRunner(
			final SparkSession session ) {
		this.session = session;
	}

	public void run()
			throws InterruptedException,
			ExecutionException,
			IOException {
		leftInternalAdapterStore = leftStore.createInternalAdapterStore();
		rightInternalAdapterStore = rightStore.createInternalAdapterStore();
		leftIndexStore = leftStore.createIndexStore();
		rightIndexStore = rightStore.createIndexStore();
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

	private void writeResultsToNewAdapter()
			throws IOException {
		if (outputStore != null) {
			ByteArrayId leftJoinId = outLeftAdapterId;
			if (leftJoinId == null) {
				leftJoinId = createDefaultAdapterName(
						leftAdapterId,
						leftStore);
			}
			final FeatureDataAdapter newLeftAdapter = FeatureDataUtils.cloneFeatureDataAdapter(
					leftStore,
					leftAdapterId,
					leftJoinId);
			final PrimaryIndex[] leftIndices = leftStore.createAdapterIndexMappingStore().getIndicesForAdapter(
					leftInternalAdapterStore.getInternalAdapterId(leftAdapterId)).getIndices(
					leftIndexStore);
			newLeftAdapter.init(leftIndices);

			ByteArrayId rightJoinId = outRightAdapterId;
			if (rightJoinId == null) {
				rightJoinId = createDefaultAdapterName(
						rightAdapterId,
						rightStore);
			}
			final FeatureDataAdapter newRightAdapter = FeatureDataUtils.cloneFeatureDataAdapter(
					rightStore,
					rightAdapterId,
					rightJoinId);
			final PrimaryIndex[] rightIndices = rightStore.createAdapterIndexMappingStore().getIndicesForAdapter(
					rightInternalAdapterStore.getInternalAdapterId(rightAdapterId)).getIndices(
					rightIndexStore);
			newRightAdapter.init(rightIndices);
			// Write each feature set to new adapter and store using original
			// indexing methods.
			RDDUtils.writeRDDToGeoWave(
					sc,
					leftIndices,
					outputStore,
					newLeftAdapter,
					getLeftResults());
			RDDUtils.writeRDDToGeoWave(
					sc,
					rightIndices,
					outputStore,
					newRightAdapter,
					getRightResults());
		}
	}

	private ByteArrayId createDefaultAdapterName(
			final ByteArrayId adapterId,
			final DataStorePluginOptions storeOptions ) {
		final String defaultAdapterName = adapterId + "_joined";
		ByteArrayId defaultAdapterId = new ByteArrayId(
				defaultAdapterName);
		final AdapterStore adapterStore = storeOptions.createAdapterStore();
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
					"spark.jars",
					jar);

			if (master != "yarn") {
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
						leftAdapterId,
						null);

				final RDDOptions leftOpts = new RDDOptions();
				leftOpts.setQueryOptions(leftOptions);
				leftOpts.setMinSplits(partCount);
				leftOpts.setMaxSplits(partCount);

				NumericIndexStrategy leftStrategy = null;
				// Did the user provide a strategy for join?
				if (indexStrategy == null) {
					final PrimaryIndex[] leftIndices = leftStore.createAdapterIndexMappingStore().getIndicesForAdapter(
							leftInternalAdapterStore.getInternalAdapterId(leftAdapterId)).getIndices(
							leftIndexStore);
					if (leftIndices.length > 0) {
						leftStrategy = leftIndices[0].getIndexStrategy();
					}

				}
				else {
					leftStrategy = indexStrategy;
				}

				leftRDD = GeoWaveRDDLoader.loadIndexedRDD(
						sc,
						leftStore,
						leftOpts,
						leftStrategy);
			}

		}

		if (rightStore != null) {
			if (rightRDD == null) {
				QueryOptions rightOptions = null;

				if (rightAdapterId == null) {
					// If no adapterId provided by user grab first adapterId
					// available.
					rightAdapterId = FeatureDataUtils.getFeatureAdapterIds(
							rightStore).get(
							0);
				}

				rightOptions = new QueryOptions(
						rightAdapterId,
						null);

				final RDDOptions rightOpts = new RDDOptions();
				rightOpts.setQueryOptions(rightOptions);
				rightOpts.setMinSplits(partCount);
				rightOpts.setMaxSplits(partCount);

				NumericIndexStrategy rightStrategy = null;
				if (indexStrategy == null) {
					final PrimaryIndex[] rightIndices = rightStore
							.createAdapterIndexMappingStore()
							.getIndicesForAdapter(
									rightInternalAdapterStore.getInternalAdapterId(rightAdapterId))
							.getIndices(
									rightIndexStore);
					if (rightIndices.length > 0) {
						rightStrategy = rightIndices[0].getIndexStrategy();
					}
				}
				else {
					rightStrategy = indexStrategy;
				}

				rightRDD = GeoWaveRDDLoader.loadIndexedRDD(
						sc,
						rightStore,
						rightOpts,
						rightStrategy);

			}

		}

	}

	private void verifyCRS() {
		// TODO: Verify that both stores have matching CRS or that one CRS can
		// be transformed into the other
	}

	// Accessors and Mutators
	public GeoWaveRDD getLeftResults() {
		return joinStrategy.getLeftResults();
	}

	public GeoWaveRDD getRightResults() {
		return joinStrategy.getRightResults();
	}

	public DataStorePluginOptions getLeftStore() {
		return leftStore;
	}

	public void setLeftStore(
			final DataStorePluginOptions leftStore ) {
		this.leftStore = leftStore;
	}

	public ByteArrayId getLeftAdapterId() {
		return leftAdapterId;
	}

	public void setLeftAdapterId(
			final ByteArrayId leftAdapterId ) {
		this.leftAdapterId = leftAdapterId;
	}

	public DataStorePluginOptions getRightStore() {
		return rightStore;
	}

	public void setRightStore(
			final DataStorePluginOptions rightStore ) {
		this.rightStore = rightStore;
	}

	public ByteArrayId getRightAdapterId() {
		return rightAdapterId;
	}

	public void setRightAdapterId(
			final ByteArrayId rightAdapterId ) {
		this.rightAdapterId = rightAdapterId;
	}

	public DataStorePluginOptions getOutputStore() {
		return outputStore;
	}

	public void setOutputStore(
			final DataStorePluginOptions outputStore ) {
		this.outputStore = outputStore;
	}

	public GeomFunction getPredicate() {
		return predicate;
	}

	public void setPredicate(
			final GeomFunction predicate ) {
		this.predicate = predicate;
	}

	public NumericIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public void setIndexStrategy(
			final NumericIndexStrategy indexStrategy ) {
		this.indexStrategy = indexStrategy;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(
			final String appName ) {
		this.appName = appName;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(
			final String master ) {
		this.master = master;
	}

	public String getHost() {
		return host;
	}

	public void setHost(
			final String host ) {
		this.host = host;
	}

	public Integer getPartCount() {
		return partCount;
	}

	public void setPartCount(
			final Integer partCount ) {
		this.partCount = partCount;
	}

	public void setSession(
			final SparkSession session ) {
		this.session = session;
	}

	public ByteArrayId getOutputLeftAdapterId() {
		return outLeftAdapterId;
	}

	public void setOutputLeftAdapterId(
			final ByteArrayId outLeftAdapterId ) {
		this.outLeftAdapterId = outLeftAdapterId;
	}

	public ByteArrayId getOutputRightAdapterId() {
		return outRightAdapterId;
	}

	public void setOutputRightAdapterId(
			final ByteArrayId outRightAdapterId ) {
		this.outRightAdapterId = outRightAdapterId;
	}

	public void setLeftRDD(
			final GeoWaveIndexedRDD leftRDD ) {
		this.leftRDD = leftRDD;
	}

	public void setRightRDD(
			final GeoWaveIndexedRDD rightRDD ) {
		this.rightRDD = rightRDD;
	}

	public boolean isNegativeTest() {
		return negativeTest;
	}

	public void setNegativeTest(
			final boolean negativeTest ) {
		this.negativeTest = negativeTest;
	}

}