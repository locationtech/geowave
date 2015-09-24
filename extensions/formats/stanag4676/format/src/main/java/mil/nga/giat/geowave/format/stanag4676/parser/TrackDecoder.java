package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.InputStream;

import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackMessage;

public interface TrackDecoder
{

	public void initialize();

	public TrackMessage readNext(
			InputStream is );

}
