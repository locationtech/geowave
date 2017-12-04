package mil.nga.giat.geowave.core.cli.api;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;

public abstract class ServiceEnabledCommand<T> extends
		DefaultOperation implements
		Command
{
	protected String path = null;

	public Pair<ServiceStatus, T> executeService(
			OperationParams params )
			throws Exception {
		return ImmutablePair.of(
				ServiceStatus.OK,
				computeResults(params));
	}

	abstract public T computeResults(
			OperationParams params )
			throws Exception;

	/**
	 * this method provides a hint to the service running the command whether it
	 * should be run asynchronously or not
	 * 
	 * @return should this method be run asynchronously
	 */
	public boolean runAsync() {
		return false;
	}

	/**
	 * the method to expose as a resource
	 *
	 * @return the HTTP method
	 */
	public HttpMethod getMethod() {
		final String path = getPath();
		if (path.contains("get") || path.contains("list")) {
			return HttpMethod.GET;
		}
		return HttpMethod.POST;
	}

	/**
	 * get the path to expose as a resource
	 *
	 * @return the path (use {param} for path encoded params)
	 */
	public String getPath() {
		if (path == null) {
			path = defaultGetPath();
		}
		return path.replace(
				"geowave",
				"v0");
	}

	public String getId() {
		return defaultId();
	}

	/**
	 * this is for ease if a class wants to merely override the final portion of
	 * a resource name and not the entire path
	 *
	 * @return the final portion of a resource name
	 */
	protected String getName() {
		return null;
	}

	private String defaultId() {
		// TODO this is used by swagger and it may determine layout but its
		// uncertain

		if (getClass().isAnnotationPresent(
				GeowaveOperation.class)) {
			final GeowaveOperation op = getClass().getAnnotation(
					GeowaveOperation.class);
			return op.parentOperation().getName() + "." + op.name();
		}
		else if ((getName() != null) && !getName().trim().isEmpty()) {
			return getName();
		}
		return getClass().getTypeName();
	}

	private String defaultGetPath() {
		final Class<?> operation = getClass();
		if (operation.isAnnotationPresent(GeowaveOperation.class)) {
			return pathFor(
					operation,
					getName()).substring(
					1);
		}
		else if ((getName() != null) && !getName().trim().isEmpty()) {
			return getName();
		}
		return operation.getTypeName();
	}

	/**
	 * Get the path for a command based on the operation hierarchy Return the
	 * path as a string in the format "/first/next/next"
	 *
	 * @param operation
	 *            - the operation to find the path for
	 * @return the formatted path as a string
	 */
	private static String pathFor(
			final Class<?> operation,
			final String resourcePathOverride ) {

		// Top level of hierarchy
		if (operation == Object.class) {
			return "";
		}

		final GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
		return pathFor(
				operationInfo.parentOperation(),
				null) + "/" + resolveName(
				operationInfo.name(),
				resourcePathOverride);
	}

	private static String resolveName(
			final String operationName,
			final String resourcePathOverride ) {
		if ((resourcePathOverride == null) || resourcePathOverride.trim().isEmpty()) {
			return operationName;
		}
		return resourcePathOverride;
	}

	public static enum HttpMethod {
		GET,
		POST,
		PUT,
		PATCH,
		DELETE
	}
}
