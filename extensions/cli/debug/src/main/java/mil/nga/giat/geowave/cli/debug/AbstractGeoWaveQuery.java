package mil.nga.giat.geowave.cli.debug;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.FilterToCQLTool;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;

abstract public class AbstractGeoWaveQuery extends
		DefaultOperation implements
		Command
{
	private static Logger LOGGER = Logger.getLogger(AbstractGeoWaveQuery.class);

	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = "--indexId", required = false, description = "The name of the index (optional)", converter = StringToByteArrayConverter.class)
	private ByteArrayId indexId;

	@Parameter(names = "--adapterId", required = false, description = "Optional ability to provide an adapter ID", converter = StringToByteArrayConverter.class)
	private ByteArrayId adapterId;

	@Parameter(names = "--debug", required = false, description = "Print out additional info for debug purposes")
	private boolean debug = false;

	@Override
	public void execute(
			OperationParams params )
			throws ParseException {
		final Stopwatch stopWatch = new Stopwatch();

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <storename>");
		}

		String storeName = parameters.get(0);

		// Config file
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load store.
		StoreLoader storeOptions = new StoreLoader(
				storeName);
		if (!storeOptions.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + storeOptions.getStoreName());
		}

		DataStore dataStore;
		AdapterStore adapterStore;
		try {
			dataStore = storeOptions.createDataStore();
			adapterStore = storeOptions.createAdapterStore();

			final GeotoolsFeatureDataAdapter adapter;
			if (adapterId != null) {
				adapter = (GeotoolsFeatureDataAdapter) adapterStore.getAdapter(adapterId);
			}
			else {
				final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
				adapter = (GeotoolsFeatureDataAdapter) it.next();
				it.close();
			}
			if (debug && (adapter != null)) {
				System.out.println(adapter);
			}
			stopWatch.start();
			final long results = runQuery(
					adapter,
					adapterId,
					indexId,
					dataStore,
					debug);
			stopWatch.stop();
			System.out.println("Got " + results + " results in " + stopWatch.toString());
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to read adapter",
					e);
		}
	}

	abstract protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			DataStore dataStore,
			boolean debug );

	public static class StringToByteArrayConverter implements
			IStringConverter<ByteArrayId>
	{
		@Override
		public ByteArrayId convert(
				String value ) {
			return new ByteArrayId(
					value);
		}
	}

}
