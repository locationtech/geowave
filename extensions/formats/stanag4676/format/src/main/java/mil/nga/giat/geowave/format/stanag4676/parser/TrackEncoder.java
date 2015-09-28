package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.OutputStream;

import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

public interface TrackEncoder
{
	public void setOutputStream(
			OutputStream os );

	public void Encode(
			TrackRun run );

	public void Encode(
			TrackMessage msg );

}
