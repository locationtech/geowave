package mil.nga.giat.geowave.core.cli.parser;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.spi.OperationEntry;

public class CommandLineOperationParams extends
		ParseOnlyOperationParams
{

	private final List<OperationEntry> operationEntries;
	private final Map<String, Operation> operationMap;
	private final boolean commandPressent;
	private int successCode = 0;
	private String successMessage;
	private Throwable successException;

	public CommandLineOperationParams(
			List<OperationEntry> operationEntries,
			String[] args ) {
		super(
				args);
		this.operationEntries = Collections.unmodifiableList(operationEntries);
		operationMap = new LinkedHashMap<String, Operation>();
		boolean commandPresent = false;
		for (OperationEntry entry : operationEntries) {
			if (operationMap.containsKey(entry.getOperationName())) {
				// Code programmer error
				throw new RuntimeException(
						"Duplicate operation name: " + entry.getOperationName());
			}
			commandPresent |= entry.isCommand();
			operationMap.put(
					entry.getOperationName(),
					entry.createInstance());
		}
		this.commandPressent = commandPresent;
	}

	/**
	 * Implement parent interface to retrieve operations
	 */
	@Override
	public Map<String, Operation> getOperationMap() {
		return operationMap;
	}

	public boolean isCommandPresent() {
		return commandPressent;
	}

	public int getSuccessCode() {
		return successCode;
	}

	public void setSuccessCode(
			int successCode ) {
		this.successCode = successCode;
	}

	public String getSuccessMessage() {
		return successMessage;
	}

	public void setSuccessMessage(
			String successMessage ) {
		this.successMessage = successMessage;
	}

	public Throwable getSuccessException() {
		return successException;
	}

	public void setSuccessException(
			Throwable successException ) {
		this.successException = successException;
	}

	public List<OperationEntry> getOperationEntries() {
		return operationEntries;
	}
}
