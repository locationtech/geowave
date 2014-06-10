package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

public class Function extends
		LogicalExpression
{
	private final String identifier;

	private final LogicalExpression[] expressions;

	public Function(
			final String identifier,
			final LogicalExpression[] expressions ) {
		this.identifier = identifier;
		this.expressions = expressions;
	}

	public String getIdentifier() {
		return identifier;
	}

	public LogicalExpression[] getExpressions() {
		return expressions;
	}

	@Override
	public void accept(
			final LogicalExpressionVisitor visitor ) {
		visitor.visit(this);
	}
}
