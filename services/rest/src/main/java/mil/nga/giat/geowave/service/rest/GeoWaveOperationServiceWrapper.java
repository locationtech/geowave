package mil.nga.giat.geowave.service.rest;

import java.io.File;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.restlet.data.Form;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Patch;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
/*=======
 import java.lang.reflect.Field;
 import java.sql.Connection;
 import java.sql.DriverManager;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.SQLException;
 import java.util.List;*/

import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.rest.field.RestFieldFactory;
import mil.nga.giat.geowave.service.rest.field.RestFieldValue;

public class GeoWaveOperationServiceWrapper<T> extends
		ServerResource
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
	private final ServiceEnabledCommand<T> operation;

	public GeoWaveOperationServiceWrapper(
			final ServiceEnabledCommand<T> operation ) {
		this.operation = operation;
	}

	@Get("json")
	public T restGet()
			throws Exception {
		if (HttpMethod.GET.equals(operation.getMethod())) {
			// TODO is this null correct?
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
		if (HttpMethod.POST.equals(operation.getMethod())) {

			final Form form = new Form(
					request);
			return handleRequest(form);
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	@Delete("form:json")
	public T restDelete(
			final Representation request )
			throws Exception {
		if (HttpMethod.DELETE.equals(operation.getMethod())) {

			final Form form = new Form(
					request);
			return handleRequest(form);
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	@Patch("form:json")
	public T restPatch(
			final Representation request )
			throws Exception {
		if (HttpMethod.PATCH.equals(operation.getMethod())) {
			final Form form = new Form(
					request);
			return handleRequest(form);
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	@Put("form:json")
	public T restPut(
			final Representation request )
			throws Exception {
		if (HttpMethod.PUT.equals(operation.getMethod())) {
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
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private void injectParameters(
			final Form form,
			final Object instance )
			throws MissingArgumentException,
			InstantiationException,
			IllegalAccessException {
		final List<RestFieldValue<?>> fields = RestFieldFactory.createRestFieldValues(instance);
		for (final RestFieldValue f : fields) {

			Object objValue = null;

			if (List.class.isAssignableFrom(f.getType())) {
				final String[] parameters = getFieldValues(
						form,
						f.getName());

				objValue = Arrays.asList(parameters);
			}
			else if (f.getType().isArray()) {
				final String[] parameters = getFieldValues(
						form,
						f.getName());

				objValue = parameters;
			}
			else {
				final String strValue = getFieldValue(
						form,
						f.getName());
				if (strValue != null) {
					if (Long.class.isAssignableFrom(f.getType())) {
						objValue = Long.valueOf(strValue);
					}
					else if (Integer.class.isAssignableFrom(f.getType()) || int.class.isAssignableFrom(f.getType())) {
						objValue = Integer.valueOf(strValue);
					}
					else if (Short.class.isAssignableFrom(f.getType()) || short.class.isAssignableFrom(f.getType())) {
						objValue = Short.valueOf(strValue);
					}
					else if (Byte.class.isAssignableFrom(f.getType()) || byte.class.isAssignableFrom(f.getType())) {
						objValue = Byte.valueOf(strValue);
					}
					else if (Double.class.isAssignableFrom(f.getType()) || double.class.isAssignableFrom(f.getType())) {
						objValue = Double.valueOf(strValue);
					}
					else if (Float.class.isAssignableFrom(f.getType()) || float.class.isAssignableFrom(f.getType())) {
						objValue = Float.valueOf(strValue);
					}
					else if (Boolean.class.isAssignableFrom(f.getType()) || boolean.class.isAssignableFrom(f.getType())) {
						objValue = Boolean.valueOf(strValue);
					}
					else if (String.class.isAssignableFrom(f.getType())) {
						objValue = strValue;
					}
					else if (Enum.class.isAssignableFrom(f.getType())) {
						objValue = Enum.valueOf(
								(Class<Enum>) f.getType(),
								strValue);
					}
					else {
						throw new RuntimeException(
								"Unsupported format on field " + f.getType());
					}
				}
			}
			if (objValue != null) {
				f.setValue(objValue);
			}
			else if (f.isRequired()) {
				throw new MissingArgumentException(
						f.getName());
			}
		}
	}

	private String[] getFieldValues(
			final Form form,
			final String name ) {
		String[] val = null;
		if (form != null) {
			val = form.getValuesArray(name);
		}
		if ((val == null) || (val.length == 0)) {
			val = getQuery().getValuesArray(
					name);
		}
		String str = getFieldValue(
				form,
				name);
		if (str == null) {
			return val;
		}
		else {

			return str.split(",");
		}
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
			injectParameters(
					form,
					operation);
		}
		catch (final MissingArgumentException e) {
			setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage());
			return null;
		}

		try {
			operation.prepare(params);
			Pair<ServiceStatus, T> result = operation.executeService(params);
			switch (result.getLeft()) {
				case OK:
					setStatus(Status.SUCCESS_OK);
					break;
				case NOT_FOUND:
					setStatus(Status.CLIENT_ERROR_NOT_FOUND);
					break;
				case DUPLICATE:
					setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
					break;
				case INTERNAL_ERROR:
					setStatus(Status.SERVER_ERROR_INTERNAL);
			}
			return result.getRight();
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
