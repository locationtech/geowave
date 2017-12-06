package mil.nga.giat.geowave.core.ingest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.ingest.spark.SparkCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.spark.SparkIngestIngestDriver;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;

@GeowaveOperation(name = "sparkToGW", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Ingest supported files that already exist in HDFS or S3")
public class SparkToGeowaveCommand extends
		ServiceEnabledCommand<Void>
{

	@Parameter(description = "<input directory> <store name> <comma delimited index/group list>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VisibilityOptions ingestOptions = new VisibilityOptions();

	@ParametersDelegate
	private SparkCommandLineOptions SparkOptions = new SparkCommandLineOptions();

	@ParametersDelegate
	private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public boolean prepare(
			final OperationParams params ) {

		return true;
	}

	/**
	 * Prep the driver & run the operation.
	 *
	 * @throws Exception
	 */
	@Override
	public void execute(
			final OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <input directory> <store name> <comma delimited index/group list>");
		}

		computeResults(params);
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String inputPath,
			final String storeName,
			final String commaSeparatedIndexes ) {
		parameters = new ArrayList<String>();
		parameters.add(inputPath);
		parameters.add(storeName);
		parameters.add(commaSeparatedIndexes);
	}

	public VisibilityOptions getIngestOptions() {
		return ingestOptions;
	}

	public void setIngestOptions(
			final VisibilityOptions ingestOptions ) {
		this.ingestOptions = ingestOptions;
	}

	public SparkCommandLineOptions getSparkOptions() {
		return SparkOptions;
	}

	public void setSparkOptions(
			final SparkCommandLineOptions SparkOptions ) {
		this.SparkOptions = SparkOptions;
	}

	public LocalInputCommandLineOptions getLocalInputOptions() {
		return localInputOptions;
	}

	public void setLocalInputOptions(
			final LocalInputCommandLineOptions localInputOptions ) {
		this.localInputOptions = localInputOptions;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	@Override
	public Void computeResults(
			final OperationParams params )
			throws Exception {
		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <file or directory> <storename> <comma delimited index/group list>");
		}

		final String inputPath = parameters.get(0);
		final String inputStoreName = parameters.get(1);
		final String indexList = parameters.get(2);

		// Config file
		final File configFile = getGeoWaveConfigFile(params);

		// Driver
		final SparkIngestIngestDriver driver = new SparkIngestIngestDriver();

		// Execute
		if (!driver.runOperation(
				configFile,
				localInputOptions,
				inputStoreName,
				indexList,
				ingestOptions,
				SparkOptions,
				inputPath)) {
			throw new RuntimeException(
					"Ingest failed to execute");
		}

		return null;
	}

}
