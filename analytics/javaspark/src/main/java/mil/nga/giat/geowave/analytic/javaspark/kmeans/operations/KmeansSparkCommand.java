package mil.nga.giat.geowave.analytic.javaspark.kmeans.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.javaspark.kmeans.KMeansRunner;
import mil.nga.giat.geowave.analytic.javaspark.kmeans.KMeansUtils;
import mil.nga.giat.geowave.analytic.mapreduce.operations.AnalyticSection;
import mil.nga.giat.geowave.analytic.mapreduce.operations.options.PropertyManagementConverter;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.geotime.store.query.ScaledTemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "kmeansspark", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "KMeans Clustering via Spark ML")
public class KmeansSparkCommand extends
		ServiceEnabledCommand<Void> implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KmeansSparkCommand.class);

	@Parameter(description = "<input storename> <output storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private KMeansSparkOptions kMeansSparkOptions = new KMeansSparkOptions();

	DataStorePluginOptions inputDataStore = null;
	DataStorePluginOptions outputDataStore = null;

	// Log some timing
	Stopwatch stopwatch = new Stopwatch();

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input storename> <output storename>");
		}
		computeResults(params);

	}

	@Override
	public Void computeResults(
			OperationParams params )
			throws Exception {
		final String inputStoreName = parameters.get(0);
		final String outputStoreName = parameters.get(1);

		// Config file
		final File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load stores.
		if (inputDataStore == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find input store: " + inputStoreLoader.getStoreName());
			}
			inputDataStore = inputStoreLoader.getDataStorePlugin();
		}

		if (outputDataStore == null) {
			final StoreLoader outputStoreLoader = new StoreLoader(
					outputStoreName);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find output store: " + outputStoreLoader.getStoreName());
			}
			outputDataStore = outputStoreLoader.getDataStorePlugin();
		}

		// Save a reference to the store in the property management.
		final PersistableStore persistedStore = new PersistableStore(
				inputDataStore);
		final PropertyManagement properties = new PropertyManagement();
		properties.store(
				StoreParameters.StoreParam.INPUT_STORE,
				persistedStore);

		// Convert properties from DBScanOptions and CommonOptions
		final PropertyManagementConverter converter = new PropertyManagementConverter(
				properties);
		converter.readProperties(kMeansSparkOptions);

		final KMeansRunner runner = new KMeansRunner();
		runner.setAppName(kMeansSparkOptions.getAppName());
		runner.setMaster(kMeansSparkOptions.getMaster());
		runner.setSplits(
				kMeansSparkOptions.getMinSplits(),
				kMeansSparkOptions.getMaxSplits());
		runner.setInputDataStore(inputDataStore);
		runner.setNumClusters(kMeansSparkOptions.getNumClusters());
		runner.setNumIterations(kMeansSparkOptions.getNumIterations());

		ScaledTemporalRange scaledRange = null;

		if (kMeansSparkOptions.isUseTime()) {
			ByteArrayId adapterId = null;
			if (kMeansSparkOptions.getAdapterId() != null) {
				adapterId = new ByteArrayId(
						kMeansSparkOptions.getAdapterId());
			}

			scaledRange = KMeansUtils.setRunnerTimeParams(
					runner,
					inputDataStore,
					adapterId);

			if (scaledRange == null) {
				LOGGER.error("Failed to set time params for kmeans. Please specify a valid feature type.");
				throw new ParameterException(
						"--useTime option: Failed to set time params");
			}
		}

		if (kMeansSparkOptions.getEpsilon() != null) {
			runner.setEpsilon(kMeansSparkOptions.getEpsilon());
		}

		if (kMeansSparkOptions.getAdapterId() != null) {
			runner.setAdapterId(kMeansSparkOptions.getAdapterId());
		}

		if (kMeansSparkOptions.getCqlFilter() != null) {
			runner.setCqlFilter(kMeansSparkOptions.getCqlFilter());
		}

		stopwatch.reset();
		stopwatch.start();

		try {
			runner.run();

		}
		catch (final IOException e) {
			throw new RuntimeException(
					"Failed to execute: " + e.getMessage());
		}

		stopwatch.stop();
		LOGGER.debug("KMeans runner took " + stopwatch.getTimeString());

		final KMeansModel clusterModel = runner.getOutputModel();

		// output cluster centroids (and hulls) to output datastore
		KMeansUtils.writeClusterCentroids(
				clusterModel,
				outputDataStore,
				kMeansSparkOptions.getCentroidTypeName(),
				scaledRange);

		if (kMeansSparkOptions.isGenerateHulls()) {
			stopwatch.reset();
			stopwatch.start();

			final JavaRDD<Vector> inputCentroids = runner.getInputCentroids();
			KMeansUtils.writeClusterHulls(
					inputCentroids,
					clusterModel,
					outputDataStore,
					kMeansSparkOptions.getHullTypeName(),
					kMeansSparkOptions.isComputeHullData());

			stopwatch.stop();
			LOGGER.debug("KMeans hull generation took " + stopwatch.getTimeString());
		}
		return null;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName ) {
		parameters = new ArrayList<String>();
		parameters.add(storeName);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputDataStore;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		inputDataStore = inputStoreOptions;
	}

	public DataStorePluginOptions getOutputStoreOptions() {
		return outputDataStore;
	}

	public void setOutputStoreOptions(
			final DataStorePluginOptions outputStoreOptions ) {
		outputDataStore = outputStoreOptions;
	}

	public KMeansSparkOptions getKMeansSparkOptions() {
		return kMeansSparkOptions;
	}

	public void setKMeansSparkOptions(
			final KMeansSparkOptions kMeansSparkOptions ) {
		this.kMeansSparkOptions = kMeansSparkOptions;
	}

}