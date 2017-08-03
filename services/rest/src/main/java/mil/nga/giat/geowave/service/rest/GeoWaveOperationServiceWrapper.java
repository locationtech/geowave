package mil.nga.giat.geowave.service.rest;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.restlet.data.Form;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.annotations.RestParameters;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.rest.GeoWaveOperationServiceWrapper.MissingArgumentException;

public class GeoWaveOperationServiceWrapper<T> extends
		ServerResource
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
	private final DefaultOperation<T> operation;

	public GeoWaveOperationServiceWrapper(
			final DefaultOperation<T> operation ) {
		this.operation = operation;
	}

	@Get("json")
	public T restGet()
			throws Exception {
		if (operation.getClass().getAnnotation(
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
			final Representation request )
			throws Exception {
		if (operation.getClass().getAnnotation(
				GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.POST) {

			final Form form = new Form(
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
			final Form form )
			throws MissingArgumentException {

		for (final Field field : FieldUtils.getFieldsWithAnnotation(
				// TODO Take out this loop?
				operation.getClass(),
				Parameter.class)) {
			processField(
					form,
					field);

		}
	}

	private Field processField(
			Form form,
			Field field )
			throws MissingArgumentException {
		final Parameter parameter = field.getAnnotation(Parameter.class);

		ParametersDelegate parametersDelegate = null;
		parametersDelegate = field.getAnnotation(ParametersDelegate.class);

		if (parameter != null) {
			if (field.getType() == String.class) {
				final String value = (form == null) ? getQueryValue(field.getName()) : form.getFirstValue(field
						.getName());
				if (value != null) {
					field.setAccessible(true); // Get around restrictions on
												// private fields. JCommander
												// does this too.
					try {
						field.set(
								this,
								value);
					}
					catch (final IllegalAccessException e) {
						throw new RuntimeException(
								e);
					}
				}
				else if (parameter.required()) {
					throw new MissingArgumentException(
							field.getName());
				}
			}
			else if ((field.getType() == Boolean.class) || (field.getType() == boolean.class)) {
				final String value = (form == null) ? getQueryValue(field.getName()) : form.getFirstValue(field
						.getName());
				if (value != null) {
					field.setAccessible(true);
					try {
						field.set(
								this,
								Boolean.valueOf(value));
					}
					catch (final IllegalAccessException e) {
						throw new RuntimeException(
								e);
					}
				}
				else if (parameter.required()) {
					throw new MissingArgumentException(
							field.getName());
				}
			}
			else if ((field.getType() == Integer.class) || (field.getType() == int.class)) {
				final String value = (form == null) ? getQueryValue(field.getName()) : form.getFirstValue(field
						.getName());
				if (value != null) {
					field.setAccessible(true);
					try {
						field.set(
								this,
								Integer.valueOf(value));
					}
					catch (final IllegalAccessException e) {
						throw new RuntimeException(
								e);
					}
				}
				else if (parameter.required()) {
					throw new MissingArgumentException(
							field.getName());
				}
			}
			else if (field.getType() == List.class) {
				final RestParameters restParameters = field.getAnnotation(RestParameters.class);
				if (restParameters == null) {
					throw new RuntimeException(
							"Missing RestParameters annotation on " + field);
				}

				field.setAccessible(true);
				List<String> parameters;
				try {
					parameters = (List<String>) field.get(this);
				}
				catch (final IllegalAccessException e) {
					throw new RuntimeException(
							e);
				}
				for (final String name : restParameters.names()) {
					final String value = (form == null) ? getQueryValue(name) : form.getFirstValue(name);
					if (value == null) {
						throw new MissingArgumentException(
								name);
					}
					parameters.add(value);
				}
			}
			else {
				throw new RuntimeException(
						"Unsupported format on field " + field);
			}
			return field;
		}
		else if (parametersDelegate != null) {
			for (Field f : FieldUtils.getAllFields(field.getType())) {
				return processField(
						form,
						field);
			}
		}
		return null;
	}

	private T handleRequest(
			final Form form )
			throws Exception {
		final String configFileParameter = (form == null) ? getQueryValue("config_file") : form
				.getFirstValue("config_file");
		final File configFile = (configFileParameter != null) ? new File(
				configFileParameter) : ConfigOptions.getDefaultPropertyFile();

		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		try {
			injectParameters(form);
		}
		catch (final MissingArgumentException e) {
			setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage());
			return null;
		}

		try {
			operation.prepare(params);
			return operation.computeResults(params);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Entered an error handling a request.",
					e);
			throw e;
		}
	}

	public static class MissingArgumentException extends
			Exception
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		private MissingArgumentException(
				final String argumentName ) {
			super(
					"Missing argument: " + argumentName);
		}

	}
}
