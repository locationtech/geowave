package mil.nga.giat.geowave.core.cli.api;

import java.util.Properties;

import org.shaded.restlet.data.Status;
import org.shaded.restlet.resource.Get;
import org.shaded.restlet.resource.Post;
import org.shaded.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;

/**
 * The default operation prevents implementors from having to implement the
 * 'prepare' function, if they don't want to.
 */
public abstract class DefaultOperation<T> extends
		ServerResource implements
		Operation
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DefaultOperation.class);

	public boolean prepare(
			OperationParams params ) {
		return true;
	}

	protected abstract T computeResults(
			OperationParams params );

	@Get("json")
	public T restGet() {
		if (getClass().getAnnotation(
				GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.GET) {
			return handleRequest();
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	@Post("json")
	public T restPost() {
		if (getClass().getAnnotation(
				GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.POST) {
			return handleRequest();
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	private T handleRequest() {
		OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				ConfigOptions.getDefaultPropertyFile());
		try {
			return computeResults(params);
		}
		catch (Exception e) {
			LOGGER.error(
					"Entered an error handling a request.",
					e);
			throw e;
		}
	}

}
