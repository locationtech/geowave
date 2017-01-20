package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.InputStream;

import mil.nga.giat.geowave.format.stanag4676.parser.model.NATO4676Message;

public interface TrackDecoder
{

	public void initialize();

	public NATO4676Message readNext(
			InputStream is );

}
