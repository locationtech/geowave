package mil.nga.giat.geowave.core.cli.parser;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.prefix.PrefixedJCommander;

/**
 * Implementation for operation arguments at command line that are used to allow
 * sections and commands to modify how arguments are parsed during prepare /
 * execution stage.
 */
public class CommandLineOperationParams implements
		OperationParams
{
	private final Map<String, Object> context = new HashMap<String, Object>();
	private final Map<String, Operation> operationMap = new LinkedHashMap<String, Operation>();
	private final String[] args;
	private PrefixedJCommander commander;
	private boolean validate = true;
	private boolean allowUnknown = false;
	private boolean commandPresent;
	private int successCode = 0;
	private String successMessage;
	private Throwable successException;

	/**
	 * Constructor with input command line arguments
	 * 
	 * @param args
	 *            command line arguments
	 */
	public CommandLineOperationParams(
			String[] args ) {
		this.args = args;
	}

	/**
	 * Return passed in arguments at command line
	 * 
	 * @return passed in arguments at command line
	 */
	public String[] getArgs() {
		return this.args;
	}

	/**
	 * Implement parent interface to retrieve operations
	 */
	@Override
	public Map<String, Operation> getOperationMap() {
		return operationMap;
	}

	@Override
	public Map<String, Object> getContext() {
		return this.context;
	}

	/**
	 * Get the commander
	 * 
	 * @return commander
	 */
	public PrefixedJCommander getCommander() {
		return this.commander;
	}

	/**
	 * Set the validate parameter
	 * 
	 * @param validate
	 *            boolean flag - true if validate should be enabled, false if
	 *            disabled
	 */
	public void setValidate(
			boolean validate ) {
		this.validate = validate;
	}

	/**
	 * Set the allow unknown flag
	 * 
	 * @param allowUnknown
	 *            boolean flag - true if validate should be enabled, false if
	 *            disabled
	 */
	public void setAllowUnknown(
			boolean allowUnknown ) {
		this.allowUnknown = allowUnknown;
	}

	/**
	 * Returns if validate is enabled
	 * 
	 * @return if validate is enabled
	 */
	public boolean isValidate() {
		return validate;
	}

	/**
	 * Returns if allow unknown is enabled
	 * 
	 * @return if allown unknown is enabled
	 */
	public boolean isAllowUnknown() {
		return this.allowUnknown;
	}

	/**
	 * Set the prefixed JCommander object
	 * 
	 * @param commander
	 *            the prefixed JCommander object
	 */
	public void setCommander(
			PrefixedJCommander commander ) {
		this.commander = commander;
	}

	/**
	 * Add a new operation to the command
	 * 
	 * @param name
	 *            unique name identifying the operation
	 * @param operation
	 *            oeration being called
	 * @param isCommand
	 *            boolean specifying if argument is a command, or another
	 *            non-command argument
	 */
	public void addOperation(
			String name,
			Operation operation,
			boolean isCommand ) {
		commandPresent |= isCommand;
		this.operationMap.put(
				name,
				operation);
	}

	/**
	 * Return if a command is present
	 * 
	 * @return boolean specifying if a command is present
	 */
	public boolean isCommandPresent() {
		return commandPresent;
	}

	/**
	 * Return the success code for the operation being run
	 * 
	 * @return the success code for the operation being run
	 */
	public int getSuccessCode() {
		return successCode;
	}

	/**
	 * Set the succcess code for the operation being run
	 * 
	 * @param successCode
	 *            the succcess code
	 */
	public void setSuccessCode(
			int successCode ) {
		this.successCode = successCode;
	}

	/**
	 * Return the success message for the operation being run
	 * 
	 * @return the success message for the operation being run
	 */
	public String getSuccessMessage() {
		return successMessage;
	}

	/**
	 * Set the succcess message for the operation being run
	 * 
	 * @param successMessage
	 *            the succcess message
	 */
	public void setSuccessMessage(
			String successMessage ) {
		this.successMessage = successMessage;
	}

	/**
	 * Return the success exception for the operation being run
	 * 
	 * @return if an exception was thrown, returns the the success exception for
	 *         the operation being run
	 */
	public Throwable getSuccessException() {
		return successException;
	}

	/**
	 * Set the succcess exception throwable for the operation being run
	 * 
	 * @param successException
	 *            Throwable that was thrown
	 */
	public void setSuccessException(
			Throwable successException ) {
		this.successException = successException;
	}
}
