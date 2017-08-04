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
import scala.actors.threadpool.Arrays;

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
			final Form form,
			final Field field )
			throws MissingArgumentException {
		final Parameter parameter = field.getAnnotation(Parameter.class);

		ParametersDelegate parametersDelegate = null;
		parametersDelegate = field.getAnnotation(ParametersDelegate.class);

		if (parameter != null) {
			if (field.getType() == String.class) {
				final String value = getFieldValue(
						form,
						field.getName());
				if (value != null) {
					field.setAccessible(true); // Get around restrictions on
												// private fields. JCommander
												// does this too.
					try {
						field.set(
								operation,
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
				final String value = getFieldValue(
						form,
						field.getName());
				if (value != null) {
					field.setAccessible(true);
					try {
						field.set(
								operation,
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
				final String value = getFieldValue(
						form,
						field.getName());
				if (value != null) {
					field.setAccessible(true);
					try {
						field.set(
								operation,
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
				field.setAccessible(true);
				String[] parameters = getFieldValues(
						form,
						field.getName());

				try {
					field.set(
							operation,
							Arrays.asList(parameters));
				}
				catch (final IllegalAccessException e) {
					throw new RuntimeException(
							e);
				}
			}
			else {
				throw new RuntimeException(
						"Unsupported format on field " + field);
			}
			return field;
		}
		else if (parametersDelegate != null) {
			for (final Field f : FieldUtils.getAllFields(field.getType())) {
				return processField(
						form,
						f);
			}
		}
		return null;
	}

	private String[] getFieldValues(
			final Form form,
			final String name ) {
		String[] val = null;
		if (form != null) {
			val = form.getValuesArray(name);
		}
		if (val == null || val.length == 0) {
			val = getQuery().getValuesArray(
					name);
		}
		return val;
	}

	private String getFieldValue(
			final Form form,
			final String name ) {
		String val = null;
		if (form != null) {
			val = form.getFirstValue(name);
		}
		if (val == null) {
			val = getQueryValue(name);
		}
		return val;
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
