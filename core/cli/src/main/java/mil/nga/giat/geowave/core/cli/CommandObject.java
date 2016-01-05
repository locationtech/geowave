package mil.nga.giat.geowave.core.cli;

import com.beust.jcommander.JCommander;

public interface CommandObject
{
	public String getName();

	public void init(
			JCommander commander );

	public boolean isHelp();
}
