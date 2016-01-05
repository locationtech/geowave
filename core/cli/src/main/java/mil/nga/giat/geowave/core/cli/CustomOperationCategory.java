package mil.nga.giat.geowave.core.cli;

public class CustomOperationCategory implements
		CLIOperationCategory
{
	private final String name;

	public CustomOperationCategory(
			final String name ) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

}
