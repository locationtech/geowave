package mil.nga.giat.geowave.service.rest;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

/**
 * Holds necessary information to create a Restlet route
 */
public class RestRoute implements
		Comparable<RestRoute>
{
	private final String path;
	private final ServiceEnabledCommand<?> operation;

	/**
	 * Create a new route given an operation
	 *
	 * @param operation
	 */
	public RestRoute(
			final ServiceEnabledCommand<?> operation ) {
		path = operation.getPath();
		this.operation = operation;
	}

	/**
	 * Return the operation as it was originally passed
	 *
	 * @return
	 */
	public ServiceEnabledCommand<?> getOperation() {
		return operation;
	}

	/**
	 * Get the path that represents the route
	 *
	 * @return a string representing the path, specified by pathFor
	 */
	public String getPath() {
		return path;
	}

	@Override
	public int compareTo(
			final RestRoute route ) {
		return path.compareTo(route.path);
	}

	@Override
	public boolean equals(
			final Object route ) {
		return (route instanceof RestRoute) && path.equals(((RestRoute) route).path);
	}

	@Override
	public int hashCode() {
		return path.hashCode();
	}
}
