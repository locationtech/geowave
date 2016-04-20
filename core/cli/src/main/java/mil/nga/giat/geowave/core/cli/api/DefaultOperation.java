package mil.nga.giat.geowave.core.cli.api;

/**
 * The default operation prevents implementors from having to implement the
 * 'prepare' function, if they don't want to.
 */
public class DefaultOperation implements
		Operation
{
	public boolean prepare(
			OperationParams params ) {
		return true;
	}
}
