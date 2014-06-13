package mil.nga.giat.geowave.ingest;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;

public class VectorFileIngest extends
		AbstractGeotoolsIngest
{
	private final static Logger LOGGER = Logger.getLogger(VectorFileIngest.class);

	public static void main(
			String[] args ) {
		Options options = getCommandLineOptions();
		CommandLineParser parser = new BasicParser();
		try {
			CommandLine line = parser.parse(
					options,
					args);
			VectorFileIngest ingester = createVectorFileIngest(line);
			ingester.ingest();
		}
		catch (MissingOptionException e) {
			System.out.println("Missing required parameters: ");
			for (Object o : e.getMissingOptions()){
				if (o instanceof OptionGroup) {
					OptionGroup og = (OptionGroup)o;
					for (Object o2 : og.getOptions()){
						Option opt = (Option)o2;
						System.out.println(String.format("  -%s,--%s:  %s", opt.getOpt(), opt.getLongOpt(), opt.getDescription()));
					}
				}
			}
			HelpFormatter fmt = new HelpFormatter();
			fmt.printHelp("java -jar geowave-ingest-geotools-datastore <options....>", options);
			
			
		}
		catch (ParseException e) {
			LOGGER.warn(
					"Unable to parse arguments",
					e);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to connect to Accumulo instance",
					e);
		}
		catch (MalformedURLException e) {
			LOGGER.warn(
					"Invalid URL for vector resource",
					e);
		}
		
	}

	private final URL ingestResource;
	private final File ingestDirectory;

	public VectorFileIngest(
			URL ingestResource,
			String zookeepers,
			String instanceId,
			String user,
			String password,
			String visibility,
			String namespace,
			IndexType type )
			throws AccumuloException,
			AccumuloSecurityException {
		super(
				new BasicAccumuloOperations(
						zookeepers,
						instanceId,
						user,
						password,
						namespace),
				type,
				visibility);
		this.ingestResource = ingestResource;
		this.ingestDirectory = null;
	}

	public VectorFileIngest(
			File ingestDirectory,
			String zookeepers,
			String instanceId,
			String user,
			String password,
			String visibility,
			String namespace,
			IndexType type )
			throws AccumuloException,
			AccumuloSecurityException {
		super(
				new BasicAccumuloOperations(
						zookeepers,
						instanceId,
						user,
						password,
						namespace),
				type,
				visibility);
		this.ingestDirectory = ingestDirectory;
		this.ingestResource = null;
	}

	public static Options getCommandLineOptions() {
		Options allOptions = new Options();
		OptionGroup input = new OptionGroup();
		input.addOption(new Option(
				"f",
				"file",
				true,
				"A file of one of the supported vector formats for ingest"));
		input.addOption(new Option(
				"d",
				"directory",
				true,
				"A directory that will be crawled for any files of a supported vector formats to be ingested"));
		input.addOption(new Option(
				"r",
				"resource",
				true,
				"A URL to a resource in one of the supported vector formats for ingest"));
		input.setRequired(true);
		allOptions.addOptionGroup(input);
		Option zookeeperUrl = new Option(
				"z",
				"zookeepers",
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		zookeeperUrl.setRequired(true);
		allOptions.addOption(zookeeperUrl);
		Option instanceId = new Option(
				"i",
				"instance-id",
				true,
				"The Accumulo instance ID");
		instanceId.setRequired(true);
		allOptions.addOption(instanceId);
		Option user = new Option(
				"u",
				"user",
				true,
				"A valid Accumulo user ID");
		user.setRequired(true);
		allOptions.addOption(user);
		Option password = new Option(
				"p",
				"password",
				true,
				"The password for the user");
		password.setRequired(true);
		allOptions.addOption(password);
		Option visibility = new Option(
				"v",
				"visibility",
				true,
				"The visiblity of the data ingested (optional; default is 'public')");
		allOptions.addOption(visibility);

		Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The table namespace (optional; default is no namespace)");
		allOptions.addOption(namespace);
		Option indexType = new Option(
				"t",
				"type",
				true,
				"The type of index, either 'spatial' or 'spatial-temporal' (optional; default is 'spatial')");
		allOptions.addOption(indexType);
		return allOptions;
	}

	public static VectorFileIngest createVectorFileIngest(
			CommandLine line )
			throws AccumuloException,
			AccumuloSecurityException,
			MalformedURLException {
		String zookeepers = line.getOptionValue("z");
		String instanceId = line.getOptionValue("i");
		String user = line.getOptionValue("u");
		String password = line.getOptionValue("p");
		String visibility = null;
		if (line.hasOption("v")) {
			visibility = line.getOptionValue("v");
		}
		String namespace = line.getOptionValue(
				"n",
				"");
		URL resource;
		String typeValue = line.getOptionValue(
				"t",
				"spatial");
		IndexType type = IndexType.SPATIAL;
		if (typeValue.equalsIgnoreCase("spatial-temporal")) {
			type = IndexType.SPATIAL_TEMPORAL;
		}
		if (line.hasOption("f")) {
			String file = line.getOptionValue("f");
			resource = new File(
					file).toURI().toURL();
		}
		else if (line.hasOption("r")) {
			resource = new URL(
					line.getOptionValue("r"));
		}
		else {
			final String directory = line.getOptionValue("d");
			return new VectorFileIngest(
					new File(
							directory),
					zookeepers,
					instanceId,
					user,
					password,
					visibility,
					namespace,
					type);
		}

		return new VectorFileIngest(
				resource,
				zookeepers,
				instanceId,
				user,
				password,
				visibility,
				namespace,
				type);
	}

	public boolean ingest() {
		if (ingestResource != null) {
			try {
				return ingestUrl(ingestResource);
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to ingest resource '" + ingestResource.toString() + "'",
						e);
			}

		}
		else if (ingestDirectory != null && ingestDirectory.exists() && ingestDirectory.isDirectory()) {
			return ingestDirectory(ingestDirectory);
		}
		else {
			LOGGER.warn("Unable to ingest vector file, a valid resource or directory must be provided");
		}

		return false;
	}

	private boolean ingestDirectory(
			File directory ) {
		boolean success = true;
		if (directory.isDirectory()) {
			// crawl directory
			for (File childFile : directory.listFiles()) {
				if (childFile.exists()) {
					if (childFile.isDirectory()) {
						ingestDirectory(childFile);
					}
					else {
						try {
							ingestUrl(childFile.toURI().toURL());
						}
						catch (IOException e) {
							LOGGER.warn(
									"Unable to read file",
									e);
							success = false;
							// its unsuccessful, but keep crawling and trying
							// other files if necessary
						}
					}
				}
			}
		}
		return success;
	}

	private boolean ingestUrl(
			URL url )
			throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(
				"url",
				url);
		DataStore dataStore = DataStoreFinder.getDataStore(map);
		if (dataStore != null) {
			return ingestDataStore(dataStore);
		}
		LOGGER.warn("Feature not found associated with URL '" + url + "'");
		return false;
	}

}
