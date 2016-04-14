package mil.nga.giat.geowave.core.cli.api;

/**
 * An operation may choose to implement Command, which will then lead to the
 * 'execute' method being called during the execute() phase.
 */
public interface Command extends
		Operation
{
	/**
	 * Execute the command, and return whether we want to continue execution
	 * 
	 * @return
	 */
	void execute(
			OperationParams params )
			throws Exception;
}
