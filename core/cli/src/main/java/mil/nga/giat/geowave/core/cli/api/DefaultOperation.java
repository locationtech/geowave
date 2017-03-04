package mil.nga.giat.geowave.core.cli.api;

import org.shaded.restlet.resource.ServerResource;

/**
 * The default operation prevents implementors from having to implement the
 * 'prepare' function, if they don't want to.
 */
public class DefaultOperation extends
		ServerResource implements
		Operation
{
	public boolean prepare(
			OperationParams params ) {
		return true;
	}
}
