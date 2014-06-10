package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

public class Value extends
		LogicalExpression
{

	public static enum ValueType {
		INTEGER,
		STRING,
		DATE_TIME,
		FLOAT,
		BOOLEAN
	}

	private final String text;

	private final ValueType type;

	public Value(
			final String text,
			final ValueType type ) {
		this.text = text;
		this.type = type;
	}

	public String getText() {
		return text;
	}

	public ValueType getType() {
		return type;
	}

	@Override
	public void accept(
			final LogicalExpressionVisitor visitor ) {
		visitor.visit(this);
	}
}
