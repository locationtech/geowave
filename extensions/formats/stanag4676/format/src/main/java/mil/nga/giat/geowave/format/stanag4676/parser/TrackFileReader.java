package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;

import mil.nga.giat.geowave.format.stanag4676.parser.model.IDdata;
import mil.nga.giat.geowave.format.stanag4676.parser.model.NATO4676Message;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackFileReader implements
		TrackReader
{

	private TrackDecoder decoder;
	private static Logger LOGGER = LoggerFactory.getLogger(TrackFileReader.class);
	private boolean streaming = false;
	private ProcessMessage handler = null;
	private TrackRun run = new TrackRun();
	private String filename;

	// init is called by spring AFTER the decoder has been set in the
	// track-services.xml
	public void init() {
		if (decoder instanceof NATO4676Decoder) {
			LOGGER.info("*** 4676 enabled " + this.getClass().toString() + " Initialized***");
		}
	}

	@Override
	public void setDecoder(
			final TrackDecoder decoder ) {
		this.decoder = decoder;
	}

	@Override
	public void setStreaming(
			final boolean stream ) {
		streaming = stream;
	}

	@Override
	public void setHandler(
			final ProcessMessage handler ) {
		this.handler = handler;
	}

	@Override
	public void initialize(
			final String algorithm,
			final String algorithmVersion,
			final long runDate,
			final String comment,
			final boolean streaming ) {
		if (run == null) {
			run = new TrackRun();
		}
		run.clearParameters();
		run.clearMessages();

		run.setAlgorithm(algorithm);
		run.setAlgorithmVersion(algorithmVersion);
		run.setRunDate(runDate);
		run.setComment(comment);
	}

	public void setFilename(
			final String filename ) {
		this.filename = filename;
	}

	public void read(
			final byte[] input ) {
		final InputStream bis = new ByteArrayInputStream(
				input);
		run.setUuid(new UUID(
				Arrays.hashCode(input),
				Arrays.hashCode(input)));
		run.setRunDate(System.currentTimeMillis());
		run.setComment("ByteArray Input");
		handler.initialize(run);
		read(bis);
		{
			try {
				bis.close();
			}
			catch (final Exception e2) {}
		}

	}

	@Override
	public void read() {
		FileInputStream fis = null;
		// Open the filename
		try {
			final File f = new File(
					filename);
			run.setUuid(UUID.randomUUID());
			run.setRunDate(f.lastModified());
			run.setComment("Track source is " + filename);
			run.setSourceFilename(filename);
			handler.initialize(run);
			fis = new FileInputStream(
					f);
			read(fis);
		}
		catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			{
				try {
					fis.close();
				}
				catch (final Exception e2) {}
			}
		}
	}

	public void read(
			final InputStream is ) {
		decoder.initialize(); // make sure the track hash is clear
		if (run == null) {
			run = new TrackRun();
		}
		final IDdata sender = new IDdata();
		sender.setStationId("GeoWave");
		sender.setNationality("US");
		NATO4676Message msg = null;
		boolean finished = false;
		while (!finished) {
			msg = decoder.readNext(is);
			if (msg != null) {
				msg.setSenderID(sender);
				if (streaming) {
					try {
						handler.notify(msg);
					}
					catch (final Exception ex) {
						ex.printStackTrace();
					}
				}
				else {
					run.addMessage(msg);
				}
			}
			else {
				finished = true;
			}
		}
		// NOTE: In Streaming mode, the run will be EMPTY
		handler.notify(run);
	}

}
