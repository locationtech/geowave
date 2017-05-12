package mil.nga.giat.geowave.core.ingest.operations;

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
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;

@GeowaveOperation(name = "localToHdfs", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Stage supported files in local file system to HDFS")
public class LocalToHdfsCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<file or directory> <hdfs host:port> <path to base directory to write to>")
	private List<String> parameters = new ArrayList<String>();

	// This helper is used to load the list of format SPI plugins that will be
	// used
	@ParametersDelegate
	private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

	@ParametersDelegate
	private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

	@Override
	public boolean prepare(
			OperationParams params ) {

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
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <file or directory> <hdfs host:port> <path to base directory to write to>");
		}

		String inputPath = parameters.get(0);
		String hdfsHostPort = parameters.get(1);
		String basePath = parameters.get(2);

		// Ensures that the url starts with hdfs://
		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}

		// Ingest Plugins
		Map<String, AvroFormatPlugin<?, ?>> ingestPlugins = pluginFormats.createAvroPlugins();

		// Driver
		StageToHdfsDriver driver = new StageToHdfsDriver(
				ingestPlugins,
				hdfsHostPort,
				basePath,
				localInputOptions);

		// Execute
		if (!driver.runOperation(inputPath)) {
			throw new RuntimeException(
					"Ingest failed to execute");
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String fileOrDirectory,
			String hdfsHostPort,
			String hdfsPath ) {
		parameters = new ArrayList<String>();
		parameters.add(fileOrDirectory);
		parameters.add(hdfsHostPort);
		parameters.add(hdfsPath);
	}

	public IngestFormatPluginOptions getPluginFormats() {
		return pluginFormats;
	}

	public void setPluginFormats(
			IngestFormatPluginOptions pluginFormats ) {
		this.pluginFormats = pluginFormats;
	}

	public LocalInputCommandLineOptions getLocalInputOptions() {
		return localInputOptions;
	}

	public void setLocalInputOptions(
			LocalInputCommandLineOptions localInputOptions ) {
		this.localInputOptions = localInputOptions;
	}
}
