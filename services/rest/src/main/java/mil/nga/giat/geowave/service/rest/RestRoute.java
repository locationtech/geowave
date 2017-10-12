package mil.nga.giat.geowave.service.rest;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;

/**
 * Holds necessary information to create a Restlet route
 */
public class RestRoute implements
		Comparable<RestRoute>
{
	private final String path;
	private final Class<?> operation;

	/**
	 * Create a new route given an operation
	 *
	 * @param operation
	 */
	public RestRoute(
			final Class<?> operation ) {
		this.path = pathFor(
				operation).substring(
				1);
		this.operation = operation;
	}

	/**
	 * Return the operation as it was originally passed
	 *
	 * @return
	 */
	public Class<?> getOperation() {
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

	/**
	 * Get the path for a command based on the operation hierarchy Return the
	 * path as a string in the format "/first/next/next"
	 *
	 * @param operation
	 *            - the operation to find the path for
	 * @return the formatted path as a string
	 */
	public static String pathFor(
			final Class<?> operation ) {

		// Top level of hierarchy
		if (operation == Object.class) {
			return "";
		}

		final GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
		return pathFor(operationInfo.parentOperation()) + "/" + operationInfo.name();
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
