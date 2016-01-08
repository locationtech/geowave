package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public interface CommandLineOptions
{
	public String getOptionValue(
			String optionName );

	public String getOptionValue(
			String optionName,
			String defaultValue );

	public boolean hasOption(
			String optionName );

	public String[] getArgs();

	public Option[] getOptions();

	public static class CommandLineWrapper implements
			CommandLineOptions
	{
		private final CommandLine commandLine;

		public CommandLineWrapper(
				final CommandLine commandLine ) {
			this.commandLine = commandLine;
		}

		@Override
		public String getOptionValue(
				final String optionName ) {
			return commandLine.getOptionValue(optionName);
		}

		@Override
		public String getOptionValue(
				final String optionName,
				final String defaultValue ) {
			return commandLine.getOptionValue(
					optionName,
					defaultValue);
		}

		@Override
		public Option[] getOptions() {
			return commandLine.getOptions();
		}

		@Override
		public boolean hasOption(
				final String optionName ) {
			return commandLine.hasOption(optionName);
		}

		@Override
		public String[] getArgs() {
			return commandLine.getArgs();
		}
	}

	public static class OptionMapWrapper implements
			CommandLineOptions
	{
		private final Map<String, String> optionMap;

		public OptionMapWrapper(
				final Map<String, String> optionMap ) {
			this.optionMap = optionMap;
		}

		@Override
		public String getOptionValue(
				final String optionName ) {
			return optionMap.get(optionName);
		}

		@Override
		public String getOptionValue(
				final String optionName,
				final String defaultValue ) {
			if (hasOption(optionName)) {
				return optionMap.get(optionName);
			}
			return defaultValue;
		}

		@Override
		public boolean hasOption(
				final String optionName ) {
			return optionMap.containsKey(optionName);
		}

		@Override
		public String[] getArgs() {
			final String[] args = new String[optionMap.size() * 2];
			int i = 0;
			for (Map.Entry<String, String> entry : optionMap.entrySet()) {
				args[i++] = "-" + entry.getKey();
				args[i++] = entry.getValue();
			}
			return args;
		}

		@Override
		public Option[] getOptions() {
			return new Option[] {};
		}
	}
}
