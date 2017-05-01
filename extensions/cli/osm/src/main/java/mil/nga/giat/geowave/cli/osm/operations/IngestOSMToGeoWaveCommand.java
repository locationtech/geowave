package mil.nga.giat.geowave.cli.osm.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.cli.osm.mapreduce.Convert.OSMConversionRunner;
import mil.nga.giat.geowave.cli.osm.mapreduce.Ingest.OSMRunner;
import mil.nga.giat.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import mil.nga.giat.geowave.cli.osm.osmfeature.types.features.FeatureDefinitionSet;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "ingest", parentOperation = OSMSection.class)
@Parameters(commandDescription = "Ingest and convert OSM data from HDFS to GeoWave")
public class IngestOSMToGeoWaveCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<hdfs host:port> <path to base directory to read from> <store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private OSMIngestCommandArgs ingestOptions = new OSMIngestCommandArgs();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <hdfs host:port> <path to base directory to read from> <store name>");
		}

		String hdfsHostPort = parameters.get(0);
		String basePath = parameters.get(1);
		String inputStoreName = parameters.get(2);

		// Ensures that the url starts with hdfs://
		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}

		if (!basePath.startsWith("/")) {
			throw new ParameterException(
					"HDFS Base path must start with forward slash /");
		}

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Copy over options from main parameter to ingest options
		ingestOptions.setHdfsBasePath(basePath);
		ingestOptions.setNameNode(hdfsHostPort);

		if (inputStoreOptions.getGeowaveNamespace() == null) {
			inputStoreOptions.getFactoryOptions().setGeowaveNamespace(
					"osmnamespace");
		}

		if (ingestOptions.getVisibilityOptions().getVisibility() == null) {
			ingestOptions.getVisibilityOptions().setVisibility(
					"public");
		}

		// This is needed by a method in OSMIngsetCommandArgs.
		ingestOptions.setOsmNamespace(inputStoreOptions.getGeowaveNamespace());

		// Ingest the data.
		ingestData();

		// Convert the data
		convertData();
	}

	private void ingestData()
			throws Exception {

		OSMRunner runner = new OSMRunner(
				ingestOptions,
				inputStoreOptions);

		int res = ToolRunner.run(
				runner,
				new String[] {});
		if (res != 0) {
			throw new RuntimeException(
					"OSMRunner failed: " + res);
		}

		System.out.println("finished ingest");
		System.out.println("**************************************************");
	}

	private void convertData()
			throws Exception {

		FeatureDefinitionSet.initialize(new OSMIngestCommandArgs().getMappingContents());

		OSMConversionRunner runner = new OSMConversionRunner(
				ingestOptions,
				inputStoreOptions);

		int res = ToolRunner.run(
				runner,
				new String[] {});
		if (res != 0) {
			throw new RuntimeException(
					"OSMConversionRunner failed: " + res);
		}

		System.out.println("finished conversion");
		System.out.println("**************************************************");
		System.out.println("**************************************************");
		System.out.println("**************************************************");
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String hdfsHostPort,
			String hdfsPath,
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(hdfsHostPort);
		this.parameters.add(hdfsPath);
		this.parameters.add(storeName);
	}

	public OSMIngestCommandArgs getIngestOptions() {
		return ingestOptions;
	}

	public void setIngestOptions(
			OSMIngestCommandArgs ingestOptions ) {
		this.ingestOptions = ingestOptions;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}
}
