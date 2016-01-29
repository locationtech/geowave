package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.split.AccumuloCommandLineOptions;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Stopwatch;

abstract public class AbstractGeoWaveQuery implements
		CLIOperationDriver
{

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		final Stopwatch stopWatch = new Stopwatch();
		final Options allOptions = new Options();
		AccumuloCommandLineOptions.applyOptions(allOptions);
		final Option adapterIdOpt = new Option(
				"adapterId",
				true,
				"Optional ability to provide an adapter ID");
		adapterIdOpt.setRequired(false);
		allOptions.addOption(adapterIdOpt);
		applyOptions(allOptions);

		final Option debugOpt = new Option(
				"debug",
				false,
				"Print out additional info for debug purposes");
		debugOpt.setRequired(false);
		allOptions.addOption(debugOpt);

		final Option indexIdOpt = new Option(
				"indexId",
				true,
				"The name of the index (optional)");
		indexIdOpt.setRequired(false);
		allOptions.addOption(indexIdOpt);
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				allOptions,
				args);
		final AccumuloCommandLineOptions cli = AccumuloCommandLineOptions.parseOptions(commandLine);
		parseOptions(commandLine);
		ByteArrayId adapterId = null;
		if (commandLine.hasOption("adapterId")) {
			adapterId = new ByteArrayId(
					commandLine.getOptionValue("adapterId"));
		}
		final boolean debug = commandLine.hasOption("debug");
		ByteArrayId indexId = null;
		if (commandLine.hasOption("indexId")) {
			indexId = new ByteArrayId(
					commandLine.getOptionValue("indexId"));
		}

		DataStore dataStore;
		AdapterStore adapterStore;
		try {
			dataStore = new AccumuloDataStore(
					cli.getAccumuloOperations());
			adapterStore = new AccumuloAdapterStore(
					cli.getAccumuloOperations());

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
			return true;
		}
		catch (AccumuloException | AccumuloSecurityException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	abstract protected void applyOptions(
			Options options );

	abstract protected void parseOptions(
			CommandLine commandLine );

	abstract protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			DataStore dataStore,
			boolean debug );

}
