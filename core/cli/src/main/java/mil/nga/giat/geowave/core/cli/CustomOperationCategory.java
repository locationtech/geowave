package mil.nga.giat.geowave.core.cli;

public class CustomOperationCategory implements
		CLIOperationCategory
{

	private final String categoryId;
	private final String name;
	private final String description;

	public CustomOperationCategory(
			final String categoryId,
			final String name,
			final String description ) {
		this.categoryId = categoryId;
		this.name = name;
		this.description = description;
	}

	@Override
	public String getCategoryId() {
		return categoryId;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getDescription() {
		return description;
	}
}
