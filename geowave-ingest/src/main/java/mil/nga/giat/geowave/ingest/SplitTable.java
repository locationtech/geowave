package mil.nga.giat.geowave.ingest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.accumulo.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;

public class SplitTable
{
	public static void main(
			final String[] args ) {
		final Options options = getCommandLineOptions();
		final CommandLineParser parser = new BasicParser();
		try {
			final CommandLine line = parser.parse(
					options,
					args);

			final String zookeepers = line.getOptionValue("z");
			final String instanceId = line.getOptionValue("i");
			final String user = line.getOptionValue("u");
			final String password = line.getOptionValue("p");
			String visibility = null;
			if (line.hasOption("v")) {
				visibility = line.getOptionValue("v");
			}
			final String namespace = line.getOptionValue(
					"n",
					"");
			final String typeValue = line.getOptionValue(
					"t",
					"spatial");
			IndexType type = IndexType.SPATIAL;
			if (typeValue.equalsIgnoreCase("spatial-temporal")) {
				type = IndexType.SPATIAL_TEMPORAL;
			}
			final Instance inst = new ZooKeeperInstance(
					instanceId,
					zookeepers);
			final Connector connector = inst.getConnector(
					user,
					password);
			final BatchScanner batchScanner = connector.createBatchScanner(
					AccumuloUtils.getQualifiedTableName(
							namespace,
							StringUtils.stringFromBinary(type.createDefaultIndex().getId().getBytes())),
					new Authorizations(
							visibility),
					16);
			batchScanner.setRanges(Arrays.asList(new Range[] {
				new Range()
			}));
			Iterator<Entry<Key, Value>> it = batchScanner.iterator();
			long count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					namespace,
					StringUtils.stringFromBinary(type.createDefaultIndex().getId().getBytes()));
			final Scanner scanner = connector.createScanner(
					tableName,
					new Authorizations(
							visibility));
			scanner.setRange(new Range());
			it = scanner.iterator();
			long i = 0;
			final double splitCount = 12;
			final double splitInterval = count / splitCount;
			final SortedSet<Text> splits = new TreeSet<Text>();
			while (it.hasNext()) {
				final Entry<Key, Value> entry = it.next();
				i++;
				if (i > splitInterval) {
					i = 0;
					splits.add(Range.followingPrefix(entry.getKey().getRow()));
				}
			}
			connector.tableOperations().addSplits(
					tableName,
					splits);
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
		}
		catch (ParseException | AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Options getCommandLineOptions() {
		final Options allOptions = new Options();
		final Option zookeeperUrl = new Option(
				"z",
				"zookeepers",
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		zookeeperUrl.setRequired(true);
		allOptions.addOption(zookeeperUrl);
		final Option instanceId = new Option(
				"i",
				"instance-id",
				true,
				"The Accumulo instance ID");
		instanceId.setRequired(true);
		allOptions.addOption(instanceId);
		final Option user = new Option(
				"u",
				"user",
				true,
				"A valid Accumulo user ID");
		user.setRequired(true);
		allOptions.addOption(user);
		final Option password = new Option(
				"p",
				"password",
				true,
				"The password for the user");
		password.setRequired(true);
		allOptions.addOption(password);
		final Option visibility = new Option(
				"v",
				"visibility",
				true,
				"The visiblity of the data ingested (optional; default is 'public')");
		allOptions.addOption(visibility);

		final Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The table namespace (optional; default is no namespace)");
		allOptions.addOption(namespace);
		final Option indexType = new Option(
				"t",
				"type",
				true,
				"The type of index, either 'spatial' or 'spatial-temporal' (optional; default is 'spatial')");
		allOptions.addOption(indexType);
		return allOptions;
	}
}
