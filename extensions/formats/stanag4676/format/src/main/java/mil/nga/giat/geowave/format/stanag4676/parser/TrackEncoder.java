package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.OutputStream;

import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

public interface TrackEncoder
{
	public void setOutputStreams(
			OutputStream trackOut,
			OutputStream missionOut );

	public void Encode(
			TrackRun run );

}
