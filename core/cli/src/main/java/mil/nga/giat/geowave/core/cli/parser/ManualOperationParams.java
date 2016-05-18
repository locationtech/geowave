package mil.nga.giat.geowave.core.cli.parser;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

public class ManualOperationParams implements
		OperationParams
{

	private final Map<String, Object> context = new HashMap<String, Object>();

	@Override
	public Map<String, Operation> getOperationMap() {
		return new HashMap<String, Operation>();
	}

	@Override
	public Map<String, Object> getContext() {
		return context;
	}
}
