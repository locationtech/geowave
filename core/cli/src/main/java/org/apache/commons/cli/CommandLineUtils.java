package org.apache.commons.cli;

public class CommandLineUtils
{
	public static void addOptions(
			final CommandLine commandLine,
			final Option[] options ) {
		for (final Option o : options) {
			commandLine.addOption(o);
		}
	}
}
