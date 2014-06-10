package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

import java.text.DateFormat;
import java.text.ParseException;

import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.AbstractExpression;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.CustomFunction;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.CustomValue;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.Expression;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model.BinaryExpression.BinaryExpressionType;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.math.NumberUtils;

public class EvaluationVisitor extends
		AbstractExpression implements
		LogicalExpressionVisitor
{
	public EvaluationVisitor(
			final Expression parentExpression ) {
		super(
				parentExpression);
	}

	protected Object result;

	public Object getResult() {
		return result;
	}

	private Object evaluate(
			final LogicalExpression expression ) {
		expression.accept(this);
		return result;
	}

	@Override
	public void visit(
			final LogicalExpression expression ) {
		// throw new Exception("The method or operation is not implemented.");
	}

	@Override
	public void visit(
			final BinaryExpression expression ) {
		// Evaluates the left expression and saves the value
		expression.getLeftExpression().accept(
				this);
		Object left = result;
		if (!(left instanceof Number) && NumberUtils.isNumber(left.toString())) {
			left = NumberUtils.toDouble(left.toString());
		}

		// Evaluates the right expression and saves the value
		expression.getRightExpression().accept(
				this);
		Object right = result;
		if (!(right instanceof Number) && NumberUtils.isNumber(right.toString())) {
			right = NumberUtils.toDouble(right.toString());
		}
		switch (expression.getType()) {
			case AND:
			case OR:
				final Boolean leftBool = BooleanUtils.toBoolean(left.toString());
				final Boolean rightBool = BooleanUtils.toBoolean(right.toString());
				if (expression.getType().equals(
						BinaryExpressionType.AND)) {
					result = leftBool && rightBool;
				}
				else {
					result = leftBool || rightBool;
				}
				break;

			case DIV:
				result = ((Number) left).doubleValue() / ((Number) right).doubleValue();
				break;

			case EQUAL:
				result = left.equals(right);
				break;

			case GREATER:
				left = convertTrueFalseStringToBoolean(left);
				right = convertTrueFalseStringToBoolean(right);
				result = ((Comparable) left).compareTo(right) > 0;
				break;
			case GREATER_THAN_OR_EQUAL:
				left = convertTrueFalseStringToBoolean(left);
				right = convertTrueFalseStringToBoolean(right);
				result = ((Comparable) left).compareTo(right) >= 0;
				break;
			case LESSER:
				left = convertTrueFalseStringToBoolean(left);
				right = convertTrueFalseStringToBoolean(right);
				result = ((Comparable) left).compareTo(right) < 0;
				break;
			case LESS_THAN_OR_EQUAL:
				left = convertTrueFalseStringToBoolean(left);
				right = convertTrueFalseStringToBoolean(right);
				result = ((Comparable) left).compareTo(right) <= 0;
				break;

			case MINUS:
				result = ((Number) left).doubleValue() - ((Number) right).doubleValue();
				break;

			case MODULO:
				result = ((Number) left).doubleValue() % ((Number) right).doubleValue();
				break;

			case NOT_EQUAL:
				result = !left.equals(right);
				break;

			case PLUS:
				result = ((Number) left).doubleValue() + ((Number) right).doubleValue();
				break;

			case TIMES:
				result = ((Number) left).doubleValue() * ((Number) right).doubleValue();
				break;

			case POW:
				result = Math.pow(
						((Number) left).doubleValue(),
						((Number) right).doubleValue());
				break;

		}
	}

	private static Object convertTrueFalseStringToBoolean(
			Object obj ) {
		if (!(obj instanceof Number) && !(obj instanceof Boolean)) {
			if (obj.toString().trim().toLowerCase().equals(
					"true")) {
				obj = new Boolean(
						true);
			}
			else if (obj.toString().trim().toLowerCase().equals(
					"false")) {
				obj = new Boolean(
						false);
			}
		}
		return obj;
	}

	@Override
	public void visit(
			final UnaryExpression expression ) {
		// Recursively evaluates the underlying expression
		expression.getExpression().accept(
				this);

		switch (expression.getType()) {
			case NOT:
				result = !(new Boolean(
						result.toString()));
				break;

			case NEGATE:
				result = -(new Double(
						result.toString()));
				break;
		}
	}

	@Override
	public void visit(
			final Value expression ) {
		switch (expression.getType()) {
			case BOOLEAN:
				result = new Boolean(
						expression.getText());
				break;

			case DATE_TIME:
				try {
					result = DateFormat.getDateTimeInstance().parse(
							expression.getText());
				}
				catch (final ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;

			case FLOAT:
				result = new Double(
						expression.getText());
				break;

			case INTEGER:
				// for now, for ease just treat every numeric as a double
				result = new Double(
						expression.getText());
				break;

			case STRING:
				result = expression.getText();
				break;
		}
	}

	@Override
	public void visit(
			final Function function ) {
		// Evaluates all parameters
		final Object[] parameters = new Object[function.getExpressions().length];
		for (int i = 0; i < parameters.length; i++) {
			parameters[i] = evaluate(function.getExpressions()[i]);
		}
		final CustomFunction customFunction = customFunctions.get(function.getIdentifier());
		if (customFunction != null) {
			// Calls external implementation
			result = customFunction.evaluate(parameters);
			// If an external implementation was used get the result back
			if (result != null) {
				return;
			}
		}

		switch (function.getIdentifier()) {
			case "abs":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"abs() takes exactly 1 argument");
				}

				result = Math.abs(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "acos":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"acos() takes exactly 1 argument");
				}

				result = Math.acos(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "asin":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"asin() takes exactly 1 argument");
				}

				result = Math.asin(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "atan":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"atan() takes exactly 1 argument");
				}

				result = Math.atan(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "ceil":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"ceil() takes exactly 1 argument");
				}

				result = Math.ceil(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "cos":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"cos() takes exactly 1 argument");
				}

				result = Math.cos(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "exp":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"exp() takes exactly 1 argument");
				}

				result = Math.exp(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "floor":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"floor() takes exactly 1 argument");
				}

				result = Math.floor(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;
			case "IEEEremainder":

				if (function.getExpressions().length != 2) {
					throw new IllegalArgumentException(
							"IEEEremainder() takes exactly 2 arguments");
				}

				result = Math.IEEEremainder(
						new Double(
								evaluate(
										function.getExpressions()[0]).toString()),
						new Double(
								evaluate(
										function.getExpressions()[1]).toString()));

				break;

			case "log":

				if (function.getExpressions().length != 2) {
					throw new IllegalArgumentException(
							"log() takes exactly 2 arguments");
				}

				result = Math.log(new Double(
						evaluate(
								function.getExpressions()[0]).toString())) / Math.log(new Double(
						evaluate(
								function.getExpressions()[1]).toString()));

				break;

			case "log10":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"log10() takes exactly 1 argument");
				}

				result = Math.log10(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "pow":

				if (function.getExpressions().length != 2) {
					throw new IllegalArgumentException(
							"pow() takes exactly 2 arguments");
				}

				result = Math.pow(
						new Double(
								evaluate(
										function.getExpressions()[0]).toString()),
						new Double(
								evaluate(
										function.getExpressions()[1]).toString()));

				break;

			case "round":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"round() takes exactly 1 argument");
				}

				result = Math.round(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "sign":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"sign() takes exactly 1 argument");
				}

				result = Math.signum(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "sin":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"sin() takes exactly 1 argument");
				}

				result = Math.sin(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "sqrt":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"sqrt() takes exactly 1 argument");
				}

				result = Math.sqrt(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "tan":

				if (function.getExpressions().length != 1) {
					throw new IllegalArgumentException(
							"tan() takes exactly 1 argument");
				}

				result = Math.tan(new Double(
						evaluate(
								function.getExpressions()[0]).toString()));

				break;

			case "max":

				if (function.getExpressions().length != 2) {
					throw new IllegalArgumentException(
							"max() takes exactly 2 arguments");
				}

				final Double maxleft = new Double(
						evaluate(
								function.getExpressions()[0]).toString());
				final Double maxright = new Double(
						evaluate(
								function.getExpressions()[1]).toString());

				result = Math.max(
						maxleft,
						maxright);
				break;

			case "min":

				if (function.getExpressions().length != 2) {
					throw new IllegalArgumentException(
							"min() takes exactly 2 arguments");
				}

				final Double minleft = new Double(
						evaluate(
								function.getExpressions()[0]).toString());
				final Double minright = new Double(
						evaluate(
								function.getExpressions()[1]).toString());

				result = Math.min(
						minleft,
						minright);
				break;

			case "if":

				if (function.getExpressions().length != 3) {
					throw new IllegalArgumentException(
							"if() takes exactly 3 arguments");
				}

				final Boolean cond = new Boolean(
						evaluate(
								function.getExpressions()[0]).toString());
				final Object then = evaluate(function.getExpressions()[1]);
				final Object els = evaluate(function.getExpressions()[2]);

				result = cond ? then : els;
				break;

			default:
				throw new IllegalArgumentException(
						"Function not found '" + function.getIdentifier() + "'");
		}
	}

	@Override
	public void visit(
			final Parameter parameter ) {
		Object value = parameters.get(parameter.getName());
		if (value != null) {
			if (value instanceof Expression) {
				// The parameter is itself another Expression
				final Expression newExpression = (Expression) parameters.get(parameter.getName());
				final Expression wrappedExpression = new Expression(
						newExpression.getExpression(),
						this);

				result = wrappedExpression.evaluate();
			}
			else {
				result = value;
			}
		}
		else {

			// Calls external implementation
			final CustomValue customValue = customValues.get(parameter.getName());
			if (customValue == null) {
				throw new IllegalArgumentException(
						"Parameter was not defined '" + parameter.getName() + "'");
			}
			value = customValue.getValue();
			if (value == null) {
				throw new IllegalArgumentException(
						"Parameter '" + parameter.getName() + "' was defined as a custom value but did not provide a valid value");
			}

			result = value;
		}
	}
}
