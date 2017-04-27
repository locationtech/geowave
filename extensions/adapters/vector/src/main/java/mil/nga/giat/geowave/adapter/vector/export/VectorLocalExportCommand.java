package mil.nga.giat.geowave.adapter.vector.export;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.adapter.vector.AvroFeatureUtils;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.avro.AttributeValues;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import mil.nga.giat.geowave.adapter.vector.cli.VectorSection;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

@GeowaveOperation(name = "localexport", parentOperation = VectorSection.class)
@Parameters(commandDescription = "Export data directly")
public class VectorLocalExportCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VectorLocalExportOptions options = new VectorLocalExportOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	public void execute(
			OperationParams params )
			throws IOException,
			CQLException {

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <store name>");
		}

		String storeName = parameters.get(0);

		// Config file
		File configFile = getGeoWaveConfigFile(params);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					storeName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		AdapterStore adapterStore = inputStoreOptions.createAdapterStore();
		IndexStore indexStore = inputStoreOptions.createIndexStore();
		DataStore dataStore = inputStoreOptions.createDataStore();

		try (final DataFileWriter<AvroSimpleFeatureCollection> dfw = new DataFileWriter<AvroSimpleFeatureCollection>(
				new GenericDatumWriter<AvroSimpleFeatureCollection>(
						AvroSimpleFeatureCollection.SCHEMA$))) {
			dfw.setCodec(CodecFactory.snappyCodec());
			dfw.create(
					AvroSimpleFeatureCollection.SCHEMA$,
					options.getOutputFile());
			// get appropriate feature adapters
			final List<GeotoolsFeatureDataAdapter> featureAdapters = new ArrayList<GeotoolsFeatureDataAdapter>();
			if ((options.getAdapterIds() != null) && !options.getAdapterIds().isEmpty()) {
				for (final String adapterId : options.getAdapterIds()) {
					final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
							adapterId));
					if (adapter == null) {
						JCommander.getConsole().println(
								"Type '" + adapterId + "' not found");
						continue;
					}
					else if (!(adapter instanceof GeotoolsFeatureDataAdapter)) {
						JCommander.getConsole().println(
								"Type '" + adapterId + "' does not support vector export. Instance of "
										+ adapter.getClass());
						continue;
					}
					featureAdapters.add((GeotoolsFeatureDataAdapter) adapter);
				}
			}
			else {
				final CloseableIterator<DataAdapter<?>> adapters = adapterStore.getAdapters();
				while (adapters.hasNext()) {
					final DataAdapter<?> adapter = adapters.next();
					if (adapter instanceof GeotoolsFeatureDataAdapter) {
						featureAdapters.add((GeotoolsFeatureDataAdapter) adapter);
					}
				}
				adapters.close();
			}
			if (featureAdapters.isEmpty()) {
				JCommander.getConsole().println(
						"Unable to find any vector data types in store");
			}
			PrimaryIndex queryIndex = null;
			if (options.getIndexId() != null) {
				final Index index = indexStore.getIndex(new ByteArrayId(
						options.getIndexId()));
				if (index == null) {
					JCommander.getConsole().println(
							"Unable to find index '" + options.getIndexId() + "' in store");
					return;
				}
				if (index instanceof PrimaryIndex) {
					queryIndex = (PrimaryIndex) index;
				}
				else {
					JCommander.getConsole().println(
							"Index '" + options.getIndexId() + "' is not a primary index");
					return;
				}
			}
			for (final GeotoolsFeatureDataAdapter adapter : featureAdapters) {
				final SimpleFeatureType sft = adapter.getFeatureType();
				JCommander.getConsole().println(
						"Exporting type '" + sft.getTypeName() + "'");
				final QueryOptions queryOptions = new QueryOptions();
				if (queryIndex != null) {
					queryOptions.setIndex(queryIndex);
				}
				Query queryConstraints = null;
				if (options.getCqlFilter() != null) {
					queryConstraints = CQLQuery.createOptimalQuery(
							options.getCqlFilter(),
							adapter,
							queryIndex,
							null);
				}
				queryOptions.setAdapter(adapter);

				final CloseableIterator<Object> it = dataStore.query(
						queryOptions,
						queryConstraints);
				int iteration = 0;
				while (it.hasNext()) {
					final AvroSimpleFeatureCollection simpleFeatureCollection = new AvroSimpleFeatureCollection();

					simpleFeatureCollection.setFeatureType(AvroFeatureUtils.buildFeatureDefinition(
							null,
							sft,
							null,
							""));
					final List<AttributeValues> avList = new ArrayList<AttributeValues>(
							options.getBatchSize());
					while (it.hasNext() && (avList.size() < options.getBatchSize())) {
						final Object obj = it.next();
						if (obj instanceof SimpleFeature) {
							final AttributeValues av = AvroFeatureUtils.buildAttributeValue(
									(SimpleFeature) obj,
									sft);
							avList.add(av);
						}
					}
					JCommander.getConsole().println(
							"Exported " + (avList.size() + (iteration * options.getBatchSize())) + " features from '"
									+ sft.getTypeName() + "'");
					iteration++;
					simpleFeatureCollection.setSimpleFeatureCollection(avList);
					dfw.append(simpleFeatureCollection);
					dfw.flush();
				}
				JCommander.getConsole().println(
						"Finished exporting '" + sft.getTypeName() + "'");
			}
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	public void setOptions(
			VectorLocalExportOptions options ) {
		this.options = options;
	}

	public VectorLocalExportOptions getOptions() {
		return options;
	}
}
