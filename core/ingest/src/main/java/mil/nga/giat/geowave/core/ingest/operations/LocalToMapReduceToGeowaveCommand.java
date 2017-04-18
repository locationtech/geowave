package mil.nga.giat.geowave.core.ingest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.StageToHdfsDriver;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.MapReduceCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "localToMrGW", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Copy supported files from local file system to HDFS and ingest from HDFS")
public class LocalToMapReduceToGeowaveCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<file or directory> <hdfs host:port> <path to base directory to write to> <store name> <comma delimited index/group list>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VisibilityOptions ingestOptions = new VisibilityOptions();

	@ParametersDelegate
	private MapReduceCommandLineOptions mapReduceOptions = new MapReduceCommandLineOptions();

	@ParametersDelegate
	private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

	// This helper is used to load the list of format SPI plugins that will be
	// used
	@ParametersDelegate
	private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	private List<IndexPluginOptions> inputIndexOptions = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);

		// Based on the selected formats, select the format plugins
		pluginFormats.selectPlugin(localInputOptions.getFormats());

		return true;
	}

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 5) {
			throw new ParameterException(
					"Requires arguments: <file or directory> <hdfs host:port> <path to base directory to write to> <store name> <comma delimited index/group list>");
		}

		if (mapReduceOptions.getJobTrackerOrResourceManagerHostPort() == null) {
			throw new ParameterException(
					"Requires job tracker or resource manager option (try geowave help <command>...)");
		}

		String inputPath = parameters.get(0);
		String hdfsHostPort = parameters.get(1);
		String basePath = parameters.get(2);
		String inputStoreName = parameters.get(3);
		String indexList = parameters.get(4);

		// Ensures that the url starts with hdfs://
		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}

		// Config file
		File configFile = getGeoWaveConfigFile(params);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Load the Indexes
		if (inputIndexOptions == null) {
			IndexLoader indexLoader = new IndexLoader(
					indexList);
			if (!indexLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + indexList);
			}
			inputIndexOptions = indexLoader.getLoadedIndexes();
		}

		// Ingest Plugins
		Map<String, AvroFormatPlugin<?, ?>> avroIngestPlugins = pluginFormats.createAvroPlugins();

		// Ingest Plugins
		Map<String, IngestFromHdfsPlugin<?, ?>> hdfsIngestPlugins = pluginFormats.createHdfsIngestPlugins();

		{

			// Driver
			StageToHdfsDriver driver = new StageToHdfsDriver(
					avroIngestPlugins,
					hdfsHostPort,
					basePath,
					localInputOptions);

			// Execute
			if (!driver.runOperation(inputPath)) {
				throw new RuntimeException(
						"Ingest failed to execute");
			}
		}

		{
			// Driver
			IngestFromHdfsDriver driver = new IngestFromHdfsDriver(
					inputStoreOptions,
					inputIndexOptions,
					ingestOptions,
					mapReduceOptions,
					hdfsIngestPlugins,
					hdfsHostPort,
					basePath);

			// Execute
			if (!driver.runOperation()) {
				throw new RuntimeException(
						"Ingest failed to execute");
			}
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String fileOrDirectory,
			String hdfsHostPort,
			String pathToBaseDirectory,
			String storeName,
			String indexList ) {
		parameters = new ArrayList<String>();
		parameters.add(fileOrDirectory);
		parameters.add(hdfsHostPort);
		parameters.add(pathToBaseDirectory);
		parameters.add(storeName);
		parameters.add(indexList);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	public List<IndexPluginOptions> getInputIndexOptions() {
		return inputIndexOptions;
	}

	public void setInputIndexOptions(
			List<IndexPluginOptions> inputIndexOptions ) {
		this.inputIndexOptions = inputIndexOptions;
	}

	public VisibilityOptions getIngestOptions() {
		return ingestOptions;
	}

	public void setIngestOptions(
			VisibilityOptions ingestOptions ) {
		this.ingestOptions = ingestOptions;
	}

	public MapReduceCommandLineOptions getMapReduceOptions() {
		return mapReduceOptions;
	}

	public void setMapReduceOptions(
			MapReduceCommandLineOptions mapReduceOptions ) {
		this.mapReduceOptions = mapReduceOptions;
	}

	public LocalInputCommandLineOptions getLocalInputOptions() {
		return localInputOptions;
	}

	public void setLocalInputOptions(
			LocalInputCommandLineOptions localInputOptions ) {
		this.localInputOptions = localInputOptions;
	}

	public IngestFormatPluginOptions getPluginFormats() {
		return pluginFormats;
	}

	public void setPluginFormats(
			IngestFormatPluginOptions pluginFormats ) {
		this.pluginFormats = pluginFormats;
	};
}
