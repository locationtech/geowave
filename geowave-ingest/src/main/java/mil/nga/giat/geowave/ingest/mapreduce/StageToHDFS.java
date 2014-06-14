package mil.nga.giat.geowave.ingest.mapreduce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.start.classloader.vfs.providers.HdfsFileSystem;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/***
 * Moves files from a local file system to HDFS - specifically into a sequence
 * file. The format of the sequence file is Key: Absolute file name (full path +
 * filename) Value: byte[] of file data, as read off disk.
 * 
 * The extension search is recursive, and will identify all files under the
 * directory matching the supplied criteria
 */
public class StageToHDFS {

	private final static Logger log = Logger.getLogger(StageToHDFS.class);
	private final static CommandLineParser parser = new BasicParser();


	public static void main( final String[] args ) {
		final Configuration conf = new Configuration();
		CommandLine line = null;
		final Options options = getOptions();
		try {
			line = parser.parse(options, args);
		} catch (final ParseException e) {
			log.fatal(e.getLocalizedMessage());
			System.exit(-1);
		}

		int matchedOptions = 0;
		for (final Option o : line.getOptions()) {

			if (o.getOpt().equals("h")) {
				final HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("4676 Ingester", options);
				System.exit(0);
			}

			if (options.hasOption(o.getOpt())) {
				matchedOptions++;
			}

		}

		if (matchedOptions != (options.getOptions().size() - 1)) {
			System.out.println("Error, all required options were not provided");
			final HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("4676 Ingester", options);
			System.exit(0);
			System.exit(-2);
		}
		String[] extensions = null;
		try {
			extensions = line.getOptionValue("x").split(",");
			
		} catch (Exception ex){
			System.out.println("Error parsing extensions argument, error was:");
			log.fatal(ex.getLocalizedMessage());
			System.exit(-3);
		}

		conf.set("fs.default.name", "hdfs://" + line.getOptionValue("hdfs"));
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		stageToHadoop(line.getOptionValue("inputPath"), line.getOptionValue("destination"), extensions, conf);

	}

	private static void stageToHadoop( final String localBaseDirectory, final String sequenceFilePath, final String[] extensions, final Configuration conf ) {
		final Path path = new Path(sequenceFilePath);
		final Text key = new Text();
		final BytesWritable value = new BytesWritable();
		SequenceFile.Writer writer = null;

		Path hdfsBaseDirectory = new Path(sequenceFilePath).getParent();
		
		
		try {
			FileSystem fs = FileSystem.get(conf);
			if (!fs.exists(hdfsBaseDirectory)){
				fs.mkdirs(hdfsBaseDirectory);
			}
		} catch (IOException ex) {
			System.out.println("Unable to create remote HDFS directory");
			log.fatal(ex.getLocalizedMessage());
			System.exit(2);
		}
		
		
		
		final Collection<File> files = FileUtils.listFiles(new File(localBaseDirectory), extensions, true);

		int percent = 0;
		int fileCount = 0;
		
		

		try {
			writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()));

		} catch (final Exception ex) {
			System.out.println("Unable to create sequence file writer");
			log.fatal(ex.getLocalizedMessage());
			System.exit(1);
		}

		long time = System.currentTimeMillis();

		for (final File f : files) {
			key.set(f.getAbsolutePath().toString());
			byte[] bytes = null;
			try {
				bytes = Files.readAllBytes(f.toPath());
			} catch (final IOException e) {
				System.out.println("Unable to read file: " + f.getAbsolutePath());
				log.error(e.getLocalizedMessage());
				System.exit(3);
			}

			value.set(bytes, 0, bytes.length);
			try {
				writer.append(key, value);
			} catch (final IOException e) {
				System.out.println("Unable to write to hdfs");
				e.printStackTrace();
				System.exit(4);
			}

			fileCount++;
			final int newPercent = Math.round((fileCount * 100.0f) / files.size());
			if (newPercent != percent) {
				System.out.println(newPercent + "% done");
				percent = newPercent;
			}
		}

		try {
			writer.close();
		} catch (final IOException e) {
			System.out.println("Unable to close wrtier");
			e.printStackTrace();
			System.exit(5);
		}

		time = System.currentTimeMillis() - time;
		final String timespan = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(time), TimeUnit.MILLISECONDS.toSeconds(time) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time)));
		System.out.println("Success, data transfered in " + timespan + " to " + conf.get("fs.default.name") + sequenceFilePath);
	}

	private static Options getOptions() {
		final Options options = new Options();
		options.addOption("hdfs", "hdfs", true, "HDFS hostname and port in the format hostname:port");
		options.addOption("i", "inputPath", true, "base directory to read 4676 xml files from");
		options.addOption("d", "destination", true, "fully qualified sequence file in hdfs");
		options.addOption("x", "extension", true, "file extension to move to hdfs");
		options.addOption("h", "help", false, "Display help");
		return options;
	}

}
