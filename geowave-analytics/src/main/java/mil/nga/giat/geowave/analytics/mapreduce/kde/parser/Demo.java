package mil.nga.giat.geowave.analytics.mapreduce.kde.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;

public class Demo
{
	public static void main(
			String[] args ) {
		String[] expressions = new String[] {
			"2 + 3 + 5",
			"2 * 3 + 5",
			"2 * (3 + 5)",
			"2 * (2*(2*(2+1)))",
			"10 % 3",
			"true or false",
			"false || not (false and true)",
			"3 > 2 and 1 <= (3-2)",
			"3 % 2 != 10 % 3",
			"abs(-1)",
			"acos(1)",
			"cos( [ pi])",
			"sin(sqrt( [ pi2 ] ))",
			"test ( sin( myFunction( )) )"
		};

		for (String expression : expressions) {
			Expression e = new Expression(
					expression);
			e.registerFunction(new CustomFunction() {

				@Override
				public String getName() {
					return "test";
				}

				@Override
				public Object evaluate(
						Object[] parameters ) {
					return new Double(
							((Number) parameters[0]).doubleValue() + 2);
				}
			});
			e.registerFunction(new CustomFunction() {

				@Override
				public String getName() {
					return "myFunction";
				}

				@Override
				public Object evaluate(
						Object[] parameters ) {
					return Math.PI;
				}
			});
			e.registerValue(new CustomValue() {

				@Override
				public String getName() {
					return "pi";
				}

				@Override
				public Object getValue() {
					return Math.PI;
				}

			});
			e.registerValue(new CustomValue() {

				@Override
				public String getName() {
					return "pi2";
				}

				@Override
				public Object getValue() {
					return Math.PI * Math.PI;
				}

			});
			System.err.println(expression + " = " + e.evaluate());
		}
	}
}
