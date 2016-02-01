package mil.nga.giat.geowave.cli.debug;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.split.AccumuloCommandLineOptions;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Stopwatch;

public class MinimalFullTable implements
		CLIOperationDriver
{

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		final Stopwatch stopWatch = new Stopwatch();
		final Options allOptions = new Options();
		AccumuloCommandLineOptions.applyOptions(allOptions);
		final Option indexIdOpt = new Option(
				"indexId",
				true,
				"The name of the index");
		indexIdOpt.setRequired(true);
		allOptions.addOption(indexIdOpt);
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				allOptions,
				args);
		final String indexId = commandLine.getOptionValue("indexId");
		final AccumuloCommandLineOptions cli = AccumuloCommandLineOptions.parseOptions(commandLine);

		try {
			final AccumuloOperations ops = cli.getAccumuloOperations();
			long results = 0;
			final BatchScanner scanner = ops.createBatchScanner(indexId);
			scanner.setRanges(Collections.singleton(new Range()));
			final Iterator<Entry<Key, Value>> it = scanner.iterator();
			stopWatch.start();
			while (it.hasNext()) {
				it.next();
				results++;
			}
			stopWatch.stop();
			scanner.close();
			System.out.println("Got " + results + " results in " + stopWatch.toString());

			return true;
		}
		catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
}
