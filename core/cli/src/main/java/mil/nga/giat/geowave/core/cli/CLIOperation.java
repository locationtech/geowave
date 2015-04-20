package mil.nga.giat.geowave.core.cli;

public class CLIOperation
{
	private final String commandlineOptionValue;
	private final String description;
	private final CLIOperationDriver driver;

	public CLIOperation(
			final String commandlineOptionValue,
			final String description,
			final CLIOperationDriver driver ) {
		this.commandlineOptionValue = commandlineOptionValue;
		this.description = description;
		this.driver = driver;
	}

	public String getCommandlineOptionValue() {
		return commandlineOptionValue;
	}

	public String getDescription() {
		return description;
	}

	public CLIOperationDriver getDriver() {
		return driver;
	}
}
