package mil.nga.giat.geowave.service.rest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;

import org.restlet.Application;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
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
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import mil.nga.giat.geowave.core.cli.exceptions.DuplicateEntryException;
import mil.nga.giat.geowave.core.cli.exceptions.TargetNotFoundException;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.service.rest.exceptions.MissingArgumentException;
import mil.nga.giat.geowave.service.rest.field.RequestParameters;
import mil.nga.giat.geowave.service.rest.field.RequestParametersForm;
import mil.nga.giat.geowave.service.rest.field.RequestParametersJson;
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
			// Still send query parameters for GETs to the RequestParameters
			// class, but don't check for JSON or other Form payloads.
			return handleRequest(new RequestParametersForm(
					getQuery()));
		}
		else {
			setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
			return null;
		}
	}

	@Post("form|json:json")
	public Representation restPost(
			final Representation request )
			throws Exception {
		return handleRequestWithPayload(
				HttpMethod.POST,
				request);
	}

	@Delete("form|json:json")
	public Representation restDelete(
			final Representation request )
			throws Exception {
		return handleRequestWithPayload(
				HttpMethod.DELETE,
				request);
	}

	@Patch("form|json:json")
	public Representation restPatch(
			final Representation request )
			throws Exception {
		return handleRequestWithPayload(
				HttpMethod.PATCH,
				request);
	}

	@Put("form|json:json")
	public Representation restPut(
			final Representation request )
			throws Exception {
		return handleRequestWithPayload(
				HttpMethod.PUT,
				request);
	}

	private Representation handleRequestWithPayload(
			HttpMethod requiredMethod,
			Representation request ) {
		// First check that the request is the requiredMethod, return 405 if
		// not.
		if (requiredMethod.equals(operation.getMethod())) {
			RequestParameters requestParameters;
			// Then check which MediaType is the request, which determines the
			// constructor used for RequestParameters.
			if (checkMediaType(
					MediaType.APPLICATION_JSON,
					request)) {
				try {
					requestParameters = new RequestParametersJson(
							request);
				}
				catch (IOException e) {
					setStatus(Status.SERVER_ERROR_INTERNAL);
					return null;
				}
			}
			else if (checkMediaType(
					MediaType.APPLICATION_WWW_FORM,
					request)) {
				requestParameters = new RequestParametersForm(
						new Form(
								request));
			}
			else {
				// If MediaType is not set, then the parameters are likely to be
				// found in the URL.

				requestParameters = new RequestParametersForm(
						getQuery());
			}
			// Finally, handle the request with the parameters, whose type
			// should no longer matter.
			return handleRequest(requestParameters);
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
			final RequestParameters requestParameters,
			final Object instance )
			throws MissingArgumentException,
			InstantiationException,
			IllegalAccessException {
		final List<RestFieldValue<?>> fields = RestFieldFactory.createRestFieldValues(instance);
		for (final RestFieldValue f : fields) {

			Object objValue = null;

			if (List.class.isAssignableFrom(f.getType())) {
				objValue = requestParameters.getList(f.getName());
			}
			else if (f.getType().isArray()) {
				objValue = requestParameters.getArray(f.getName());
			}
			else {
				final String strValue = (String) requestParameters.getString(f.getName());
				if (strValue != null) {
					if (Long.class.isAssignableFrom(f.getType()) || long.class.isAssignableFrom(f.getType())) {
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

	private Representation handleRequest(
			final RequestParameters parameters )
			 {

		final String configFileParameter = (parameters == null) ? getQueryValue("config_file") : (String) parameters
				.getValue("config_file");

		final File configFile = (configFileParameter != null) ? new File(
				configFileParameter) : (initContextConfigFile != null) ? new File(
						initContextConfigFile) : ConfigOptions.getDefaultPropertyFile();

		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		try {
			injectParameters(
					parameters,
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
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();	
			
			if(operation.runAsync()) {
				final Context appContext = Application.getCurrent().getContext();
				final ExecutorService opPool = (ExecutorService)appContext.getAttributes().get("asyncOperationPool");
				final ConcurrentHashMap<String, Future> opStatuses = (ConcurrentHashMap<String, Future>)appContext.getAttributes().get("asyncOperationStatuses");
				
				Callable <T> task = () -> {
					T res = operation.computeResults(params);
					return res;
				};
				final Future<T> futureResult = opPool.submit(task);
				final UUID opId = UUID.randomUUID();
				opStatuses.put(opId.toString(), futureResult);
				
				rm.status = RestOperationStatusMessage.StatusType.STARTED;
				rm.message = "Async operation started with ID in data field. Check status at /operation_status?id=";
				rm.data = opId.toString();
			} else {
				final T result = operation.computeResults(params);		
				rm.status = RestOperationStatusMessage.StatusType.COMPLETE;
				rm.data = result;
			}
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			if(operation.successStatusIs200()){
				setStatus(Status.SUCCESS_OK);
			}
			else{
				setStatus(Status.SUCCESS_CREATED);
			}
			return rep;
		}
		catch (final NotAuthorizedException e){
			LOGGER.error(
					"Entered an error handling a request.",
					e.getMessage());
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = e.getMessage();
			setStatus(Status.CLIENT_ERROR_UNAUTHORIZED);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			return rep;
		}
		catch (final ForbiddenException e){
			LOGGER.error(
					"Entered an error handling a request.",
					e.getMessage());
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = e.getMessage();
			setStatus(Status.CLIENT_ERROR_FORBIDDEN);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			return rep;
		}
		catch (final TargetNotFoundException e){
			LOGGER.error(
					"Entered an error handling a request.",
					e.getMessage());
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = e.getMessage();
			setStatus(Status.CLIENT_ERROR_NOT_FOUND);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			return rep;
		}
		catch (final DuplicateEntryException e){
			LOGGER.error(
					"Entered an error handling a request.",
					e.getMessage());
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = e.getMessage();
			setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			return rep;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Entered an error handling a request.",
					e.getMessage());
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = "exception occurred";
			rm.data = e;
			setStatus(Status.SERVER_ERROR_INTERNAL);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(rm);
			return rep;
		}
	}

	/**
	 * Checks that the desired MediaType is compatible with the one present in
	 * the request.
	 * 
	 * @param expectedType
	 *            The expected type.
	 * @param request
	 *            The request whose MediaType is being checked.
	 * @return true, if the MediaTypes match. --- OR false, if the MediaTypes do
	 *         not match, or the request is null.
	 */
	private boolean checkMediaType(
			MediaType expectedType,
			Representation request ) {
		if (request == null) {
			return false;
		}
		return expectedType.isCompatible(request.getMediaType());
	}
}
