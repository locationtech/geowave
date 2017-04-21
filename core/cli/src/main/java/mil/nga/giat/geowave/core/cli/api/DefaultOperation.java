package mil.nga.giat.geowave.core.cli.api;

/**
 * The default operation prevents implementors from having to implement the
 * 'prepare' function, if they don't want to.
 */
public class DefaultOperation implements
		Operation
{
	/**
	 * Prepare the command, and return whether we want to continue
	 * 
	 * @param params
	 *            Arguments to be used to allow sections and commands to modify
	 *            how arguments are parsed during prepare stage.
	 * @return True if operation was successfully prepared, false otherwise
	 */
	public boolean prepare(
			OperationParams params ) {
		return true;
	}
}
