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
import mil.nga.giat.geowave.core.ingest.kafka.IngestFromKafkaDriver;
import mil.nga.giat.geowave.core.ingest.kafka.KafkaConsumerCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "kafkaToGW", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Subscribe to a Kafka topic and ingest into GeoWave")
public class KafkaToGeowaveCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<store name> <comma delimited index/group list>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VisibilityOptions ingestOptions = new VisibilityOptions();

	@ParametersDelegate
	private KafkaConsumerCommandLineOptions kafkaOptions = new KafkaConsumerCommandLineOptions();

	@ParametersDelegate
	private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

	// This helper is used to load the list of format SPI plugins that will be
	// used
	@ParametersDelegate
	private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	private List<IndexPluginOptions> inputIndexOptions = null;

	protected IngestFromKafkaDriver driver = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);

		// TODO: localInputOptions has 'extensions' which doesn't mean
		// anything for Kafka to Geowave

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
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <store name> <comma delimited index/group list>");
		}

		String inputStoreName = parameters.get(0);
		String indexList = parameters.get(1);

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
		Map<String, AvroFormatPlugin<?, ?>> ingestPlugins = pluginFormats.createAvroPlugins();

		// Driver
		driver = new IngestFromKafkaDriver(
				inputStoreOptions,
				inputIndexOptions,
				ingestPlugins,
				kafkaOptions,
				ingestOptions);

		// Execute
		if (!driver.runOperation()) {
			throw new RuntimeException(
					"Ingest failed to execute");
		}
	}

	public IngestFromKafkaDriver getDriver() {
		return driver;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName,
			String commaSeparatedIndexes ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
		this.parameters.add(commaSeparatedIndexes);
	}

	public VisibilityOptions getIngestOptions() {
		return ingestOptions;
	}

	public void setIngestOptions(
			VisibilityOptions ingestOptions ) {
		this.ingestOptions = ingestOptions;
	}

	public KafkaConsumerCommandLineOptions getKafkaOptions() {
		return kafkaOptions;
	}

	public void setKafkaOptions(
			KafkaConsumerCommandLineOptions kafkaOptions ) {
		this.kafkaOptions = kafkaOptions;
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
}
