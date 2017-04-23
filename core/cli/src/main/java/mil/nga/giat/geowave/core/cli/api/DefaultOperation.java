package mil.nga.giat.geowave.core.cli.api;

import java.io.File;
import java.util.Properties;

import org.shaded.restlet.data.Form;
import org.shaded.restlet.data.Status;
import org.shaded.restlet.representation.Representation;
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
			return handleRequest(null);
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	@Post("form:json")
	public T restPost(
			Representation request ) {
		if (getClass().getAnnotation(
				GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.POST) {

			Form form = new Form(
					request);
			readFormArgs(form);
			return handleRequest(form);
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	protected void readFormArgs(
			Form form ) {}

	private T handleRequest(
			Form form ) {
		String configFileParameter = (form == null) ? getQueryValue("config_file") : form.getFirstValue("config_file");
		File configFile = (configFileParameter != null) ? new File(
				configFileParameter) : ConfigOptions.getDefaultPropertyFile();

		OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);
		try {
			prepare(params);
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
