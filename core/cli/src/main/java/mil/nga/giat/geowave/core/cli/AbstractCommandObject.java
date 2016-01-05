package mil.nga.giat.geowave.core.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

abstract public class AbstractCommandObject implements
		CommandObject
{
	@Parameter(names = "--help", description = "Display help", help = true)
	private final boolean help = false;

	@Override
	public boolean isHelp() {
		return help;
	}

	@Override
	public void init(
			final JCommander commander ) {
		// convenience for command objects that don't need to take advantage of
		// init()
	}

}
