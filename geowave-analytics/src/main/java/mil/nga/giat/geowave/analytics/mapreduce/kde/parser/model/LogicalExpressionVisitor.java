package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

public interface LogicalExpressionVisitor
{
	public void visit(
			LogicalExpression expression );

	public void visit(
			BinaryExpression expression );

	public void visit(
			UnaryExpression expression );

	public void visit(
			Value expression );

	public void visit(
			Function function );

	public void visit(
			Parameter function );
}
