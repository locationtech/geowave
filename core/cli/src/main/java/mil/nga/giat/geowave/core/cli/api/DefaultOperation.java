package mil.nga.giat.geowave.core.cli.api;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.shaded.restlet.data.Form;
import org.shaded.restlet.data.Status;
import org.shaded.restlet.representation.Representation;
import org.shaded.restlet.resource.Get;
import org.shaded.restlet.resource.Post;
import org.shaded.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.annotations.RestParameters;
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
			return handleRequest(form);
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	/**
	 * Reads Parameter fields of the current instance, and populates them with
	 * values from the request.
	 * 
	 * This uses an analogous approach to JCommander. Ideally, it could reuse
	 * the same implementation, but ParametersDelegate makes this a bit
	 * trickier, since those aren't initialized right away. Follow the behavior
	 * as best as possible, and perform validation.
	 * 
	 * @param form
	 *            The form to fetch parameters from, or the query if form is
	 *            null.
	 */
	private void injectParameters(
			Form form )
			throws MissingArgumentException {

		for (Field field : FieldUtils.getFieldsWithAnnotation(
				getClass(),
				Parameter.class)) {
			Parameter parameter = field.getAnnotation(Parameter.class);
			if (field.getType() == String.class) {
				String value = (form == null) ? getQueryValue(field.getName()) : form.getFirstValue(field.getName());
				if (value != null) {
					field.setAccessible(true); // Get around restrictions on
												// private fields. JCommander
												// does this too.
					try {
						field.set(
								this,
								value);
					}
					catch (IllegalAccessException e) {
						throw new RuntimeException(
								e);
					}
				}
				else if (parameter.required()) throw new MissingArgumentException(
						field.getName());
			}
			else if (field.getType() == Boolean.class) {
				String value = (form == null) ? getQueryValue(field.getName()) : form.getFirstValue(field.getName());
				if (value != null) {
					field.setAccessible(true);
					try {
						field.set(
								this,
								Boolean.valueOf(value));
					}
					catch (IllegalAccessException e) {
						throw new RuntimeException(
								e);
					}
				}
				else if (parameter.required()) throw new MissingArgumentException(
						field.getName());
			}
			else if (field.getType() == List.class) {
				RestParameters restParameters = field.getAnnotation(RestParameters.class);
				if (restParameters == null) throw new RuntimeException(
						"Missing RestParameters annotation on " + field);

				field.setAccessible(true);
				List<String> parameters;
				try {
					parameters = (List<String>) field.get(this);
				}
				catch (IllegalAccessException e) {
					throw new RuntimeException(
							e);
				}
				for (String name : restParameters.names()) {
					String value = (form == null) ? getQueryValue(name) : form.getFirstValue(name);
					if (value == null) throw new MissingArgumentException(
							name);
					parameters.add(value);
				}
			}
			else {
				throw new RuntimeException(
						"Unsupported format on field " + field);
			}
		}
	}

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
			injectParameters(form);
		}
		catch (MissingArgumentException e) {
			setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage());
			return null;
		}

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

	private static class MissingArgumentException extends
			Exception
	{
		private final String argumentName;

		private MissingArgumentException(
				String argumentName ) {
			super(
					"Missing argument: " + argumentName);
			this.argumentName = argumentName;
		}

	}
}
