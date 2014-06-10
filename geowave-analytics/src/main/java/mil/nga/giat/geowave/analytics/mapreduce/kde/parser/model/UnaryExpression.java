package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

public class UnaryExpression extends
		LogicalExpression
{
	public static enum UnaryExpressionType {
		NOT,
		NEGATE
	}

	private final LogicalExpression expression;

	private final UnaryExpressionType type;

	public UnaryExpression(
			final UnaryExpressionType type,
			final LogicalExpression expression ) {
		this.type = type;
		this.expression = expression;
	}

	public LogicalExpression getExpression() {
		return expression;
	}

	public UnaryExpressionType getType() {
		return type;
	}

	@Override
	public void accept(
			final LogicalExpressionVisitor visitor ) {
		visitor.visit(this);
	}
}
