package mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model;

import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.BasicMathLexer;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.BasicMathParser;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model.BinaryExpression.BinaryExpressionType;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model.UnaryExpression.UnaryExpressionType;
import mil.nga.giat.geowave.analytics.mapreduce.kde.parser.model.Value.ValueType;

import org.antlr.runtime.tree.CommonTree;

public abstract class LogicalExpression
{
	// private static final char BS = '\\';

	private static String extractString(
			final String text ) {
		return text;
		// final StringBuilder sb = new StringBuilder(
		// text);
		// int startIndex = 1; // Skip initial quote
		// int slashIndex = -1;
		//
		// while ((slashIndex = sb.toString().indexOf(
		// BS,
		// startIndex)) != -1) {
		// final char escapeType = sb.charAt(slashIndex + 1);
		// switch (escapeType) {
		// case 'u':
		// final String hcode = new String(
		// new char[] {
		// sb.charAt(slashIndex + 4),
		// sb.charAt(slashIndex + 5)
		// });
		// final String lcode = new String(
		// new char[] {
		// sb.charAt(slashIndex + 2),
		// sb.charAt(slashIndex + 3)
		// });
		// final char unicodeChar = UnicodeEscaper.between(lcode.codePointAt(0),
		// hcode.codePointAt(1));
		// sb.replace(
		// slashIndex,
		// slashIndex + 6,
		// String.format("\\u%x", cp));
		// break;
		// case 'n':
		// sb.replace(
		// slashIndex,
		// slashIndex + 2,
		// "\n");
		// break;
		// case 'r':
		// sb.replace(
		// slashIndex,
		// slashIndex + 2,
		// "\r");
		// break;
		// case 't':
		// sb.replace(
		// slashIndex,
		// slashIndex + 2,
		// "\t");
		// break;
		// case '\'':
		// sb.replace(
		// slashIndex,
		// slashIndex + 2,
		// "\'");
		// break;
		// case '\\':
		// sb.replace(
		// slashIndex,
		// slashIndex + 2,
		// "\\");
		// break;
		// default:
		// throw new ApplicationException(
		// "Unvalid escape sequence: \\" + escapeType);
		// }
		//
		// startIndex = slashIndex + 1;
		//
		// }
		//
		// sb.Remove(
		// 0,
		// 1);
		// sb.Remove(
		// sb.Length - 1,
		// 1);
		//
		// return sb.ToString();
	}

	public static LogicalExpression create(
			final CommonTree ast ) {
		if (ast == null) {
			throw new IllegalArgumentException(
					"tree is null");
		}

		switch (ast.getType()) {
			case BasicMathParser.STRING:
				return new Value(
						extractString(ast.getText()),
						ValueType.STRING);

			case BasicMathParser.INTEGER:
				return new Value(
						ast.getText(),
						ValueType.INTEGER);

			case BasicMathParser.BOOLEAN:
				return new Value(
						ast.getText(),
						ValueType.BOOLEAN);

			case BasicMathParser.DATETIME:
				return new Value(
						ast.getText(),
						ValueType.DATE_TIME);

			case BasicMathParser.FLOAT:
				return new Value(
						ast.getText(),
						ValueType.FLOAT);

			case BasicMathParser.NOT:
				return new UnaryExpression(
						UnaryExpressionType.NOT,
						create((CommonTree) ast.getChild(0)));

			case BasicMathParser.NEGATE:
				return new UnaryExpression(
						UnaryExpressionType.NEGATE,
						create((CommonTree) ast.getChild(0)));

			case BasicMathParser.MULT:
				return new BinaryExpression(
						BinaryExpressionType.TIMES,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.POW:
				return new BinaryExpression(
						BinaryExpressionType.POW,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.DIV:
				return new BinaryExpression(
						BinaryExpressionType.DIV,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.MOD:
				return new BinaryExpression(
						BinaryExpressionType.MODULO,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.PLUS:
				return new BinaryExpression(
						BinaryExpressionType.PLUS,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.MINUS:
				return new BinaryExpression(
						BinaryExpressionType.MINUS,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.LT:
				return new BinaryExpression(
						BinaryExpressionType.LESSER,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.LTEQ:
				return new BinaryExpression(
						BinaryExpressionType.LESS_THAN_OR_EQUAL,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.GT:
				return new BinaryExpression(
						BinaryExpressionType.GREATER,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.GTEQ:
				return new BinaryExpression(
						BinaryExpressionType.GREATER_THAN_OR_EQUAL,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.EQUALS:
				return new BinaryExpression(
						BinaryExpressionType.EQUAL,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.NOTEQUALS:
				return new BinaryExpression(
						BinaryExpressionType.NOT_EQUAL,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.AND:
				return new BinaryExpression(
						BinaryExpressionType.AND,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathParser.OR:
				return new BinaryExpression(
						BinaryExpressionType.OR,
						create((CommonTree) ast.getChild(0)),
						create((CommonTree) ast.getChild(1)));

			case BasicMathLexer.IDENT:
				final LogicalExpression[] expressions = new LogicalExpression[ast.getChildCount()];

				for (int i = 0; i < ast.getChildCount(); i++) {
					expressions[i] = LogicalExpression.create((CommonTree) ast.getChild(i));
				}

				return new Function(
						ast.getText(),
						expressions);

			case BasicMathLexer.PARAM:
				return new Parameter(
						((CommonTree) ast.getChild(0)).getText());

			default:
				return null;
		}

	}

	public void accept(
			final LogicalExpressionVisitor visitor ) {
		visitor.visit(this);
	}
}
