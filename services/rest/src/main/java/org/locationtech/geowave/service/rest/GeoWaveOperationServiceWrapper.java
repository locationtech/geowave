/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import org.locationtech.geowave.core.cli.exceptions.DuplicateEntryException;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.cli.utils.InstantiationUtils;
import org.locationtech.geowave.service.rest.exceptions.MissingArgumentException;
import org.locationtech.geowave.service.rest.field.RequestParameters;
import org.locationtech.geowave.service.rest.field.RequestParametersForm;
import org.locationtech.geowave.service.rest.field.RequestParametersJson;
import org.locationtech.geowave.service.rest.field.RestFieldFactory;
import org.locationtech.geowave.service.rest.field.RestFieldValue;
import org.locationtech.geowave.service.rest.operations.RestOperationStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.NoConverter;

/** Runs one request against a fresh instance of a route's operation. */
public class GeoWaveOperationServiceWrapper<T> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
  private final ServiceEnabledCommand<T> operation;
  private final String initContextConfigFile;
  private final AsyncOperations asyncOperations;

  public GeoWaveOperationServiceWrapper(
      final ServiceEnabledCommand<T> operation,
      final String initContextConfigFile,
      final AsyncOperations asyncOperations) {
    this.operation = operation;
    this.initContextConfigFile = initContextConfigFile;
    this.asyncOperations = asyncOperations;
  }

  /**
   * Handles a request made with the given method. GETs, and requests without a JSON or form body,
   * take their parameters from the query.
   *
   * @param contentType the media type of the body, or null if there is none
   * @param body the request body, or null or empty if there is none
   */
  public Response handle(
      final HttpMethod requestMethod,
      final MediaType contentType,
      final String body,
      final MultivaluedMap<String, String> query) {
    if (!requestMethod.equals(operation.getMethod())) {
      return Response.status(Status.METHOD_NOT_ALLOWED).allow(operation.getMethod().name()).build();
    }
    final RequestParameters requestParameters;
    if (HttpMethod.GET.equals(requestMethod)
        || (contentType == null)
        || (body == null)
        || body.isEmpty()) {
      requestParameters = new RequestParametersForm(query);
    } else if (contentType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
      try {
        requestParameters = new RequestParametersJson(body);
      } catch (final IOException e) {
        LOGGER.error("Unable to parse the JSON request body", e);
        return JsonResponses.of(
            Status.INTERNAL_SERVER_ERROR,
            JsonResponses.error("Unable to parse the JSON request body", null));
      }
    } else if (contentType.isCompatible(MediaType.APPLICATION_FORM_URLENCODED_TYPE)) {
      try {
        requestParameters = RequestParametersForm.fromUrlEncoded(body);
      } catch (final IllegalArgumentException e) {
        LOGGER.error("Unable to parse the form request body", e);
        return JsonResponses.of(
            Status.BAD_REQUEST,
            JsonResponses.error("Unable to parse the form request body", null));
      }
    } else {
      requestParameters = new RequestParametersForm(query);
    }
    return handleRequest(requestParameters);
  }

  /**
   * Reads Parameter fields of the current instance, and populates them with values from the
   * request.
   *
   * <p> This uses an analogous approach to JCommander. Ideally, it could reuse the same
   * implementation, but ParametersDelegate makes this a bit trickier, since those aren't
   * initialized right away. Follow the behavior as best as possible, and perform validation.
   *
   * @param requestParameters the request's parameters
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  private void injectParameters(final RequestParameters requestParameters, final Object instance)
      throws MissingArgumentException, InstantiationException, IllegalAccessException {
    final List<RestFieldValue<?>> fields = RestFieldFactory.createRestFieldValues(instance);
    for (final RestFieldValue f : fields) {

      Object objValue = null;
      final Class<?> type = f.getType();
      final Field field = f.getField();
      final String strValue = requestParameters.getString(f.getName());

      if (field.isAnnotationPresent(Parameter.class)) {
        final Class<? extends IStringConverter<?>> converter =
            field.getAnnotation(Parameter.class).converter();
        if (converter != null) {
          if ((converter != NoConverter.class) && (strValue != null)) {
            try {
              objValue = InstantiationUtils.newInstance(converter).convert(strValue);
            } catch (final InstantiationException e) {
              LOGGER.warn(
                  "Cannot convert parameter since converter does not have zero argument constructor",
                  e);
            }
          }
        }
      }

      if (objValue == null) {
        if (List.class.isAssignableFrom(type)) {
          objValue = requestParameters.getList(f.getName());
        } else if (type.isArray()) {
          objValue = requestParameters.getArray(f.getName());
          if (objValue != null) {
            objValue =
                Arrays.copyOf((Object[]) objValue, ((Object[]) objValue).length, f.getType());
          }
        } else {
          if (strValue != null) {
            if (Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type)) {
              objValue = Long.valueOf(strValue);
            } else if (Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type)) {
              objValue = Integer.valueOf(strValue);
            } else if (Short.class.isAssignableFrom(type) || short.class.isAssignableFrom(type)) {
              objValue = Short.valueOf(strValue);
            } else if (Byte.class.isAssignableFrom(type) || byte.class.isAssignableFrom(type)) {
              objValue = Byte.valueOf(strValue);
            } else if (Double.class.isAssignableFrom(type) || double.class.isAssignableFrom(type)) {
              objValue = Double.valueOf(strValue);
            } else if (Float.class.isAssignableFrom(type) || float.class.isAssignableFrom(type)) {
              objValue = Float.valueOf(strValue);
            } else if (Boolean.class.isAssignableFrom(type)
                || boolean.class.isAssignableFrom(type)) {
              objValue = Boolean.valueOf(strValue);
            } else if (String.class.isAssignableFrom(type)) {
              objValue = strValue;
            } else if (Enum.class.isAssignableFrom(type)) {
              objValue = Enum.valueOf((Class<Enum>) type, strValue.toUpperCase());
            } else {
              throw new RuntimeException("Unsupported format on field " + f.getType());
            }
          }
        }
      }
      if (objValue != null) {
        f.setValue(objValue);
      } else if (f.isRequired()) {
        throw new MissingArgumentException(f.getName());
      }
    }
  }

  private Response handleRequest(final RequestParameters parameters) {

    final String configFileParameter = (String) parameters.getValue("config_file");

    final File configFile =
        (configFileParameter != null) ? new File(configFileParameter)
            : (initContextConfigFile != null) ? new File(initContextConfigFile)
                : ConfigOptions.getDefaultPropertyFile();

    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    try {
      injectParameters(parameters, operation);
    } catch (final Exception e) {
      LOGGER.error("Could not convert parameters", e);
      return JsonResponses.of(Status.BAD_REQUEST, JsonResponses.error("exception occurred", e));
    }

    try {
      operation.prepare(params);

      try {
        injectParameters(parameters, operation);
      } catch (final Exception e) {
        LOGGER.error("Entered an error handling a request.", e);
        return JsonResponses.of(Status.BAD_REQUEST, JsonResponses.error("exception occurred", e));
      }

      final RestOperationStatusMessage rm = new RestOperationStatusMessage();

      if (operation.runAsync()) {
        rm.status = RestOperationStatusMessage.StatusType.STARTED;
        rm.message =
            "Async operation started with ID in data field. Check status at /operation_status?id=";
        rm.data = asyncOperations.submit(() -> operation.computeResults(params));
      } else {
        final T result = operation.computeResults(params);
        rm.status = RestOperationStatusMessage.StatusType.COMPLETE;
        rm.data = result;
      }
      return JsonResponses.of(operation.successStatusIs200() ? Status.OK : Status.CREATED, rm);
    } catch (final NotAuthorizedException e) {
      LOGGER.error("Entered an error handling a request.", e);
      return JsonResponses.of(Status.UNAUTHORIZED, JsonResponses.error(e.getMessage(), null));
    } catch (final ForbiddenException e) {
      LOGGER.error("Entered an error handling a request.", e);
      return JsonResponses.of(Status.FORBIDDEN, JsonResponses.error(e.getMessage(), null));
    } catch (final TargetNotFoundException e) {
      LOGGER.error("Entered an error handling a request.", e);
      return JsonResponses.of(Status.NOT_FOUND, JsonResponses.error(e.getMessage(), null));
    } catch (final DuplicateEntryException | ParameterException e) {
      LOGGER.error("Entered an error handling a request.", e);
      return JsonResponses.of(Status.BAD_REQUEST, JsonResponses.error(e.getMessage(), null));
    } catch (final Exception e) {
      LOGGER.error("Entered an error handling a request.", e);
      return JsonResponses.of(
          Status.INTERNAL_SERVER_ERROR,
          JsonResponses.error("exception occurred", e));
    }
  }
}
