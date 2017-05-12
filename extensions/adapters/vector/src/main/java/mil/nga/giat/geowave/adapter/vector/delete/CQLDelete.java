package mil.nga.giat.geowave.adapter.vector.delete;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.text.cql2.CQLException;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.cli.VectorSection;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

@GeowaveOperation(name = "cqldelete", parentOperation = VectorSection.class)
@Parameters(commandDescription = "Delete data that matches a CQL filter")
public class CQLDelete extends
		DefaultOperation implements
		Command
{
	private static Logger LOGGER = LoggerFactory.getLogger(CQLDelete.class);

	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = "--cql", required = true, description = "CQL Filter for delete")
	private String cqlStr;

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
		if (debug) {
			org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.DEBUG);
		}

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
				LOGGER.debug(adapter.toString());
			}

			stopWatch.start();
			final long results = delete(
					adapter,
					adapterId,
					indexId,
					dataStore,
					debug);
			stopWatch.stop();

			if (debug) {
				LOGGER.debug(results + " results remaining after delete; time = " + stopWatch.toString());
			}
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to read adapter",
					e);
		}
	}

	protected long delete(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		long missed = 0;

		try {
			boolean success = dataStore.delete(
					new QueryOptions(
							adapterId,
							indexId),
					mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery.createOptimalQuery(
							cqlStr,
							adapter,
							null,
							null));

			if (debug) {
				LOGGER.debug("CQL Delete " + (success ? "Success" : "Failure"));
			}
		}
		catch (CQLException e2) {
			LOGGER.warn(
					"Error parsing CQL",
					e2);
		}

		// Verify delete by running the CQL query
		if (debug) {
			try (final CloseableIterator<Object> it = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
					mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery.createOptimalQuery(
							cqlStr,
							adapter,
							null,
							null))) {

				while (it.hasNext()) {
					it.next();
					missed++;
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read result",
						e);
			}
			catch (final CQLException e1) {
				LOGGER.error(
						"Unable to create optimal query",
						e1);
			}
		}

		return missed;
	}

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
