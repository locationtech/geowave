package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

public class BinaryExpression extends
		LogicalExpression
{

	public static enum BinaryExpressionType {
		AND,
		OR,
		NOT_EQUAL,
		LESS_THAN_OR_EQUAL,
		GREATER_THAN_OR_EQUAL,
		LESSER,
		GREATER,
		EQUAL,
		MINUS,
		PLUS,
		MODULO,
		DIV,
		TIMES,
		POW,
	}

	private final LogicalExpression leftExpression;

	private final LogicalExpression rightExpression;

	private final BinaryExpressionType type;

	public BinaryExpression(
			final BinaryExpressionType type,
			final LogicalExpression leftExpression,
			final LogicalExpression rightExpression ) {
		this.type = type;
		this.leftExpression = leftExpression;
		this.rightExpression = rightExpression;
	}

	public LogicalExpression getLeftExpression() {
		return leftExpression;
	}

	public LogicalExpression getRightExpression() {
		return rightExpression;
	}

	public BinaryExpressionType getType() {
		return type;
	}

	@Override
	public void accept(
			final LogicalExpressionVisitor visitor ) {
		visitor.visit(this);
	}
}
