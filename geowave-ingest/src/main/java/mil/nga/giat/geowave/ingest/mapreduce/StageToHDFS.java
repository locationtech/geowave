package mil.nga.giat.geowave.ingest.mapreduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import mil.nga.giat.geowave.ingest.mapreduce.gpx.GPXTrack;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
	public final static String TAG_SEPARATOR = " ||| ";


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
				formatter.printHelp("GPX Ingester", options);
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

		try {
			stageToHadoop(line.getOptionValue("inputPath"), line.getOptionValue("destination"), extensions, conf);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XMLStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static Map<Long, GPXTrack> parseMetadata(File metadataFile) throws FileNotFoundException, XMLStreamException{
		Map<Long, GPXTrack> metadata = new HashMap<Long, GPXTrack>();
		final XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		XMLEventReader eventReader = null;
		InputStream in = null;
		in = new BufferedInputStream(new FileInputStream(metadataFile));
		eventReader = inputFactory.createXMLEventReader(in);
		while (eventReader.hasNext()){
			XMLEvent event = eventReader.nextEvent();
			if (event.isStartElement()){
				StartElement node = event.asStartElement();
				switch (node.getName().getLocalPart()){
					case "gpxFile" : {
						GPXTrack gt = new GPXTrack();
						node = event.asStartElement();
						final Iterator<Attribute> attributes = node.getAttributes();
						while (attributes.hasNext()) {
							final Attribute a = attributes.next();
							switch (a.getName().getLocalPart()){
								case "id" : {
									gt.setTrackid(Long.parseLong(a.getValue()));
									break;
								}
								case "timestamp" : {
									gt.setTimestamp(a.getValue());
									break;
								}
								case "points" : {
									gt.setPoints(Long.parseLong(a.getValue()));
									break;
								}
								case "visibility" : {
									gt.setVisibility(a.getValue());
									break;
								}
								case "uid" : {
									gt.setUserid(Long.parseLong(a.getValue()));
									break;
								}
								case "user" : {
									gt.setUser(a.getValue());
									break;
								}
								
							}
						}
						while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals("gpxFile"))){
							if (event.isStartElement()){
								node = event.asStartElement();
								switch (node.getName().getLocalPart()){
									case "description" : {
										event = eventReader.nextEvent();
										if (event.isCharacters()){
											gt.setDescription(event.asCharacters().getData());
										}
										break;
									}
									case "tags" : {
										List<CharSequence> tags = new ArrayList<CharSequence>();
										while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals("tags"))){
											if (event.isStartElement()){
												node = event.asStartElement();
												if (node.getName().getLocalPart().equals("tag")){
													event = eventReader.nextEvent();
													if (event.isCharacters()){
														tags.add(event.asCharacters().getData());
													}
												}
											}
											event = eventReader.nextEvent();
										}
										gt.setTags(tags);
										break;
									}
									
								}
							}
							event = eventReader.nextEvent();
						}
						metadata.put(gt.getTrackid(), gt);
						break;
					}
					
				}
			}
		}
		return metadata;
	}

	private static void stageToHadoop( final String localBaseDirectory, final String sequenceFilePath, final String[] extensions, final Configuration conf ) throws FileNotFoundException, XMLStreamException {
		final Path path = new Path(sequenceFilePath);
		

		Map<Long, GPXTrack> metadata = null;
		
		File f = new File(localBaseDirectory + "/metadata.xml");
		if (!f.exists()){
			log.warn("No metadata file found - looked at: " + f.getAbsolutePath()) ;
			log.warn("No metadata will be loaded");
		} else {
			System.out.println("Parsing metdata file");
			long time = System.currentTimeMillis();
			metadata = parseMetadata(f);
			time = System.currentTimeMillis() - time;
			final String timespan = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(time), TimeUnit.MILLISECONDS.toSeconds(time) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time)));
			System.out.println("Metadata parsed in in " + timespan + " for " + metadata.size() + " tracks" );
		}
			
		
		
		DataFileWriter<GPXTrack> dfw = new DataFileWriter<GPXTrack>(new GenericDatumWriter<GPXTrack>());
		dfw.setCodec(CodecFactory.snappyCodec());
		
		Path hdfsBaseDirectory = new Path(sequenceFilePath).getParent();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(conf);
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
		
		FSDataOutputStream out = null;
		
		try {
			out = fs.create(path);
			dfw.create(GPXTrack.getClassSchema(), out);

		} catch (final IOException ex) {
			System.out.println("Unable to create output stream");
			log.fatal(ex.getLocalizedMessage());
			System.exit(1);
		}

		long time = System.currentTimeMillis();
		
		long lastfreevalue = -1;
		
		
		for (final File gpx : files) {
			GPXTrack track = null;
			
			try {
				long id = Long.parseLong(FilenameUtils.removeExtension(gpx.getName()));
				track = metadata.get(id);
				if (track == null){
					track = new GPXTrack();
					track.setTrackid(lastfreevalue);
					lastfreevalue--;
				}
			} catch (NumberFormatException ex){
				track = new GPXTrack();
				track.setTrackid(lastfreevalue);
				lastfreevalue--;
			}
			
			byte[] bb = null;
			try {
				bb = IOUtils.toByteArray(gpx.toURI());
			} catch (IOException ex) {
				System.out.println("Unable to read local file: " + gpx.getAbsolutePath());
				log.fatal(ex.getLocalizedMessage());
				System.exit(1);
			}
						
			ByteBuffer bbuf = ByteBuffer.wrap(bb);
			track.setGpxfile(bbuf);
			
			try {
				dfw.append(track);
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
			dfw.close();
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
