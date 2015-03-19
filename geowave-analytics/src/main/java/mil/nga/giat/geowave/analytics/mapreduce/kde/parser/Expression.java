package mil.nga.giat.geowave.analytics.mapreduce.kde.parser;

import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model.EvaluationVisitor;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model.LogicalExpression;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RuleReturnScope;
import org.antlr.runtime.tree.CommonTree;

//import org.springframework.expression.EvaluationException;

public class Expression extends
		AbstractExpression
{
	private final String expression;

	public Expression(
			final String expression,
			final AbstractExpression parentExpression ) {
		super(
				parentExpression);
		if ((expression == null) || expression.isEmpty()) {
			throw new IllegalArgumentException(
					"Expression can't be empty");
		}

		this.expression = expression;
	}

	public Expression(
			final String expression ) {
		super();
		if ((expression == null) || expression.isEmpty()) {
			throw new IllegalArgumentException(
					"Expression can't be empty");
		}

		this.expression = expression;
	}

	public String getExpression() {
		return expression;
	}

	protected CommonTree parse(
			final String expression ) {
		final BasicMathLexer lexer = new BasicMathLexer(
				new ANTLRStringStream(
						expression));
		final BasicMathParser parser = new BasicMathParser(
				new CommonTokenStream(
						lexer));

		try {
			final RuleReturnScope rule = parser.expression();
			if (parser.hasError()) {
				throw new java.lang.RuntimeException(
						parser.errorMessage() + " " + parser.errorPosition());
			}

			return (CommonTree) rule.getTree();
		}
		catch (final RuntimeException e) {
			throw e;
		}
		catch (final Exception e) {
			throw new RuntimeException(
					e.getMessage(),
					e);
		}
	}

	public Object evaluate() {
		final EvaluationVisitor visitor = new EvaluationVisitor(
				this);

		LogicalExpression.create(
				parse(expression)).accept(
				visitor);
		return visitor.getResult();
	}

}
