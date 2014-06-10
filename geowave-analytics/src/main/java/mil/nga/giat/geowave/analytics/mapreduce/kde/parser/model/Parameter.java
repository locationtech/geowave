package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

public class Parameter extends
		LogicalExpression
{
	private final String name;

	public Parameter(
			final String name ) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	@Override
	public void accept(
			final LogicalExpressionVisitor visitor ) {
		visitor.visit(this);
	}
}
