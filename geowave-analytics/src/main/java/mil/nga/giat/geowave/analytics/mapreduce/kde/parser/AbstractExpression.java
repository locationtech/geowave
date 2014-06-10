package mil.nga.giat.geowave.analytics.mapreduce.kde.parser;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractExpression
{
	protected final Map<String, CustomFunction> customFunctions;
	protected final Map<String, CustomValue> customValues;
	protected final Map<String, Object> parameters;

	protected AbstractExpression() {
		this(
				new HashMap<String, CustomFunction>(),
				new HashMap<String, CustomValue>(),
				new HashMap<String, Object>());
	}

	protected AbstractExpression(
			final AbstractExpression parentExpression ) {
		this(
				parentExpression.customFunctions,
				parentExpression.customValues,
				parentExpression.parameters);
	}

	protected AbstractExpression(
			final Map<String, CustomFunction> customFunctions,
			final Map<String, CustomValue> customValues,
			final Map<String, Object> parameters ) {
		this.customFunctions = customFunctions;
		this.customValues = customValues;
		this.parameters = parameters;
	}

	public void registerFunction(
			final CustomFunction function ) {
		customFunctions.put(
				function.getName(),
				function);
	}

	public void registerValue(
			final CustomValue value ) {
		customValues.put(
				value.getName(),
				value);
	}
}
