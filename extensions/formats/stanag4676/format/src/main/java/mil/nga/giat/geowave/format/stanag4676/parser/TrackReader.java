package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.IOException;

import mil.nga.giat.geowave.format.stanag4676.parser.model.NATO4676Message;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

public interface TrackReader
{
	public interface ProcessMessage
	{
		public void initialize(
				TrackRun run );

		public void notify(
				NATO4676Message msg )
				throws InterruptedException,
				IOException;

		public void notify(
				TrackRun run );
	}

	public void setDecoder(
			TrackDecoder decoder );

	public void setStreaming(
			boolean stream );

	public void setHandler(
			ProcessMessage handler );

	public void initialize(
			String algorithm,
			String algorithmVersion,
			long runDate,
			String comment,
			boolean streaming );

	public void read();

}
