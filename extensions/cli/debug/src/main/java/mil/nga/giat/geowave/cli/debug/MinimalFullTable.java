package mil.nga.giat.geowave.cli.debug;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;

@GeowaveOperation(name = "fullscanMinimal", parentOperation = DebugSection.class)
@Parameters(commandDescription = "full table scan without any iterators or deserialization")
public class MinimalFullTable extends
		DefaultOperation implements
		Command
{
	private static Logger LOGGER = Logger.getLogger(MinimalFullTable.class);

	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = "--indexId", required = true, description = "The name of the index (optional)")
	private String indexId;

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

		String storeType = storeOptions.getDataStorePlugin().getType();

		if (storeType.equals(AccumuloDataStore.TYPE)) {
			try {
				AccumuloRequiredOptions opts = (AccumuloRequiredOptions) storeOptions.getFactoryOptions();

				final AccumuloOperations ops = new BasicAccumuloOperations(
						opts.getZookeeper(),
						opts.getInstance(),
						opts.getUser(),
						opts.getPassword(),
						opts.getGeowaveNamespace());

				long results = 0;
				final BatchScanner scanner = ops.createBatchScanner(indexId);
				scanner.setRanges(Collections.singleton(new Range()));
				Iterator<Entry<Key, Value>> it = scanner.iterator();

				stopWatch.start();
				while (it.hasNext()) {
					it.next();
					results++;
				}
				stopWatch.stop();

				scanner.close();
				System.out.println("Got " + results + " results in " + stopWatch.toString());
			}
			catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
				LOGGER.error(
						"Unable to scan accumulo datastore",
						e);
			}
		}
		else if (storeType.equals(HBaseDataStore.TYPE)) {
			throw new UnsupportedOperationException(
					"full scan for store type " + storeType + " not yet implemented.");
		}
		else {
			throw new UnsupportedOperationException(
					"full scan for store type " + storeType + " not implemented.");
		}
	}

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}
}
