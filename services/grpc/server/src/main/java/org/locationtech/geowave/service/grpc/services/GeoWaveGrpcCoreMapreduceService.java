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
import java.util.Map;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CoreMapreduceGrpc.CoreMapreduceImplBase;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcCoreMapreduceService extends CoreMapreduceImplBase implements
    GeoWaveGrpcServiceSpi {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcCoreMapreduceService.class.getName());

  @Override
  public BindableService getBindableService() {
    return this;
  }

  @Override
  public void configHDFSCommand(
      final org.locationtech.geowave.service.grpc.protobuf.ConfigHDFSCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {

    final ConfigHDFSCommand cmd = new ConfigHDFSCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing ConfigHDFSCommand...");
    try {
      cmd.computeResults(params);
      final VoidResponseProtos resp = VoidResponseProtos.newBuilder().build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }
}
