package mil.nga.giat.geowave.service.rest;

import java.io.File;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.Status;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Patch;
import org.restlet.resource.Post;
import org.restlet.resource.Put;

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
import mil.nga.giat.geowave.service.rest.operations.RestOperationStatusMessage;

public class GeoWaveOperationServiceWrapper<T> extends
		ServerResource
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
	private final ServiceEnabledCommand<T> operation;
	private final String initContextConfigFile;

	public GeoWaveOperationServiceWrapper(
			final ServiceEnabledCommand<T> operation,
			final String initContextConfigFile ) {
		this.operation = operation;
		this.initContextConfigFile = initContextConfigFile;
	}

	@Get("json")
	public Representation restGet()
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
	public Representation restPost(
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
	public Representation restDelete(
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
	public Representation restPatch(
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
	public Representation restPut(
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

	private Representation handleRequest(
			final Form form )
			 {

		final String configFileParameter = (form == null) ? getQueryValue("config_file") : form
				.getFirstValue("config_file");

		final File configFile = (configFileParameter != null) ? new File(
				configFileParameter) : (initContextConfigFile != null) ? new File(
						initContextConfigFile) : ConfigOptions.getDefaultPropertyFile();

		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		try {
			injectParameters(
					form,
					operation);
		}
		catch (final Exception e) {
			LOGGER.error("Entered an error handling a request.", e.getMessage());
			setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage());
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = "exception occurred";
			rm.data = e;
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			return rep;
		}

		try {
			operation.prepare(params);
			
			if(operation.runAsync()) {
				final Context appContext = Application.getCurrent().getContext();
				final ExecutorService opPool = (ExecutorService)appContext.getAttributes().get("asyncOperationPool");
				final ConcurrentHashMap<String, Future> opStatuses = (ConcurrentHashMap<String, Future>)appContext.getAttributes().get("asyncOperationStatuses");
				
				Callable <T> task = () -> {
					Pair<ServiceStatus, T> res = operation.executeService(params);
					return res.getRight();
				};
				final Future<T> futureResult = opPool.submit(task);
				final UUID opId = UUID.randomUUID();
				opStatuses.put(opId.toString(), futureResult);
				setStatus(Status.SUCCESS_OK);
				
				final RestOperationStatusMessage rm = new RestOperationStatusMessage();
				rm.status = RestOperationStatusMessage.StatusType.STARTED;
				rm.message = "Async operation started with ID in data field. Check status at /operation_status?id=";
				rm.data = opId.toString();
				final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
				return rep;
			} else {
				final Pair<ServiceStatus, T> result = operation.executeService(params);
				final RestOperationStatusMessage rm = new RestOperationStatusMessage();
				
				switch (result.getLeft()) {
					case OK:
						rm.status = RestOperationStatusMessage.StatusType.COMPLETE;
						setStatus(Status.SUCCESS_OK);
						break;
					case NOT_FOUND:
						rm.status = RestOperationStatusMessage.StatusType.ERROR;
						setStatus(Status.CLIENT_ERROR_NOT_FOUND);
						break;
					case DUPLICATE:
						setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
						break;
					case INTERNAL_ERROR:
						rm.status = RestOperationStatusMessage.StatusType.ERROR;
						setStatus(Status.SERVER_ERROR_INTERNAL);
				}
				
				rm.data = result.getRight();
				final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
				return rep;
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Entered an error handling a request.",
					e.getMessage());
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = "exception occurred";
			rm.data = e;
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			return rep;
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
