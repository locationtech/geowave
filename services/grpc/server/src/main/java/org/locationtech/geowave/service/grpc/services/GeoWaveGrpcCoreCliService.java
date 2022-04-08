/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.grpc.services;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.ListCommand;
import org.locationtech.geowave.core.cli.operations.config.SetCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CoreCliGrpc.CoreCliImplBase;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.MapStringStringResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcCoreCliService extends CoreCliImplBase implements GeoWaveGrpcServiceSpi {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcCoreCliService.class.getName());

  @Override
  public BindableService getBindableService() {
    return this;
  }

  @Override
  public void setCommand(
      final org.locationtech.geowave.service.grpc.protobuf.SetCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final SetCommand cmd = new SetCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing SetCommand...");
    try {
      final Object result = cmd.computeResults(params);
      String strResponseProtos = "";
      if (result != null) {
        strResponseProtos = result.toString();
      }
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(strResponseProtos).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void listCommand(
      final org.locationtech.geowave.service.grpc.protobuf.ListCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.MapStringStringResponseProtos> responseObserver) {

    final ListCommand cmd = new ListCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing ListCommand...");
    try {
      final Map<String, String> post_result = new HashMap<>();
      final Map<String, Object> result = cmd.computeResults(params);
      final Iterator<Entry<String, Object>> it = result.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<String, Object> pair = it.next();
        post_result.put(pair.getKey().toString(), pair.getValue().toString());
      }
      final MapStringStringResponseProtos resp =
          MapStringStringResponseProtos.newBuilder().putAllResponseValue(post_result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }
}
