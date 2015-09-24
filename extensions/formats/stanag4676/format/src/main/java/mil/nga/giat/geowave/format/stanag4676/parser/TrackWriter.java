package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.IOException;

import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

public interface TrackWriter
{

	public void setEncoder(
			TrackEncoder encoder );

	public void initialize(
			TrackRun run );

	public void write(
			TrackRun run )
			throws IOException;

	public void write(
			TrackMessage msg )
			throws IOException;

}
