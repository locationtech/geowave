package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.ingest.MainCommandLineOptions.Operation;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsDriver;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestDriver;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.log4j.Logger;

public class GeoWaveIngestMain {
	private final static Logger LOGGER = Logger.getLogger(GeoWaveIngestMain.class);

	public static void main( final String[] args ) {
		final Options operations = new Options();
		MainCommandLineOptions.applyOptions(operations);
		if (args.length < 1) {
			final HelpFormatter help = new HelpFormatter();
			help.printHelp("<operation> <options>", operations);
		}
		final String[] optionsArgs = new String[args.length - 1];
		System.arraycopy(args, 1, optionsArgs, 0, optionsArgs.length);
		final String[] operationsArgs = new String[] { args[0] };
		final Parser parser = new BasicParser();
		CommandLine operationCommandLine;
		try {
			operationCommandLine = parser.parse(operations, operationsArgs);
			final MainCommandLineOptions operationOption = MainCommandLineOptions.parseOptions(operationCommandLine);
			switch (operationOption.getOperation()) {
				case LOCAL_INGEST:
					final LocalFileIngestDriver localIngest = new LocalFileIngestDriver();
					localIngest.run(optionsArgs);
					break;
				case LOCAL_TO_HDFS_INGEST:
				case STAGE_TO_HDFS:
					final StageToHdfsDriver stage = new StageToHdfsDriver();
					stage.run(optionsArgs);
					if (operationOption.getOperation().equals(Operation.STAGE_TO_HDFS)) {
						// if its local to HDFS ingest continue on to the ingest
						break;
					}
				case INGEST_FROM_HDFS:
					final IngestFromHdfsDriver hdfsIngest = new IngestFromHdfsDriver();
					hdfsIngest.run(optionsArgs);
					break;

			}
		} catch (final ParseException e) {
			e.printStackTrace();
			LOGGER.fatal("Unable to parse operation", e);
		}
	}
}
