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
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.cli.geoserver.ConfigGeoServerCommand;
import org.locationtech.geowave.cli.geoserver.coverage.GeoServerAddCoverageCommand;
import org.locationtech.geowave.cli.geoserver.coverage.GeoServerGetCoverageCommand;
import org.locationtech.geowave.cli.geoserver.coverage.GeoServerListCoveragesCommand;
import org.locationtech.geowave.cli.geoserver.coverage.GeoServerRemoveCoverageCommand;
import org.locationtech.geowave.cli.geoserver.cvstore.GeoServerAddCoverageStoreCommand;
import org.locationtech.geowave.cli.geoserver.cvstore.GeoServerGetCoverageStoreCommand;
import org.locationtech.geowave.cli.geoserver.cvstore.GeoServerListCoverageStoresCommand;
import org.locationtech.geowave.cli.geoserver.cvstore.GeoServerRemoveCoverageStoreCommand;
import org.locationtech.geowave.cli.geoserver.datastore.GeoServerAddDatastoreCommand;
import org.locationtech.geowave.cli.geoserver.datastore.GeoServerGetDatastoreCommand;
import org.locationtech.geowave.cli.geoserver.datastore.GeoServerGetStoreAdapterCommand;
import org.locationtech.geowave.cli.geoserver.datastore.GeoServerListDatastoresCommand;
import org.locationtech.geowave.cli.geoserver.datastore.GeoServerRemoveDatastoreCommand;
import org.locationtech.geowave.cli.geoserver.featurelayer.GeoServerAddFeatureLayerCommand;
import org.locationtech.geowave.cli.geoserver.featurelayer.GeoServerGetFeatureLayerCommand;
import org.locationtech.geowave.cli.geoserver.featurelayer.GeoServerListFeatureLayersCommand;
import org.locationtech.geowave.cli.geoserver.featurelayer.GeoServerRemoveFeatureLayerCommand;
import org.locationtech.geowave.cli.geoserver.layer.GeoServerAddLayerCommand;
import org.locationtech.geowave.cli.geoserver.style.GeoServerAddStyleCommand;
import org.locationtech.geowave.cli.geoserver.style.GeoServerGetStyleCommand;
import org.locationtech.geowave.cli.geoserver.style.GeoServerListStylesCommand;
import org.locationtech.geowave.cli.geoserver.style.GeoServerRemoveStyleCommand;
import org.locationtech.geowave.cli.geoserver.style.GeoServerSetLayerStyleCommand;
import org.locationtech.geowave.cli.geoserver.workspace.GeoServerAddWorkspaceCommand;
import org.locationtech.geowave.cli.geoserver.workspace.GeoServerListWorkspacesCommand;
import org.locationtech.geowave.cli.geoserver.workspace.GeoServerRemoveWorkspaceCommand;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc.CliGeoserverImplBase;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.RepeatedStringResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcCliGeoserverService extends CliGeoserverImplBase implements
    GeoWaveGrpcServiceSpi {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcCliGeoserverService.class.getName());

  @Override
  public BindableService getBindableService() {
    return this;
  }

  @Override
  public void geoServerListWorkspacesCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerListWorkspacesCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.RepeatedStringResponseProtos> responseObserver) {

    final GeoServerListWorkspacesCommand cmd = new GeoServerListWorkspacesCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerListWorkspacesCommand...");
    try {
      final List<String> result = cmd.computeResults(params);
      final RepeatedStringResponseProtos resp =
          RepeatedStringResponseProtos.newBuilder().addAllResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerAddCoverageCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerAddCoverageCommand cmd = new GeoServerAddCoverageCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerAddCoverageCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerRemoveCoverageStoreCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageStoreCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerRemoveCoverageStoreCommand cmd = new GeoServerRemoveCoverageStoreCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerRemoveCoverageStoreCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerAddCoverageStoreCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageStoreCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerAddCoverageStoreCommand cmd = new GeoServerAddCoverageStoreCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerAddCoverageStoreCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerGetCoverageStoreCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageStoreCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {
    final GeoServerGetCoverageStoreCommand cmd = new GeoServerGetCoverageStoreCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerGetCoverageStoreCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerAddDatastoreCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerAddDatastoreCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {
    final GeoServerAddDatastoreCommand cmd = new GeoServerAddDatastoreCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerAddDatastoreCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerGetStyleCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStyleCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerGetStyleCommand cmd = new GeoServerGetStyleCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerGetStyleCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void configGeoServerCommand(
      final org.locationtech.geowave.service.grpc.protobuf.ConfigGeoServerCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final ConfigGeoServerCommand cmd = new ConfigGeoServerCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing ConfigGeoServerCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerGetCoverageCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {
    final GeoServerGetCoverageCommand cmd = new GeoServerGetCoverageCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerGetCoverageCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerListFeatureLayersCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerListFeatureLayersCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerListFeatureLayersCommand cmd = new GeoServerListFeatureLayersCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerListFeatureLayersCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerGetStoreAdapterCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStoreAdapterCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.RepeatedStringResponseProtos> responseObserver) {

    final GeoServerGetStoreAdapterCommand cmd = new GeoServerGetStoreAdapterCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerGetStoreAdapterCommand...");
    try {
      final List<String> result = cmd.computeResults(params);
      final RepeatedStringResponseProtos resp =
          RepeatedStringResponseProtos.newBuilder().addAllResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerAddWorkspaceCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerAddWorkspaceCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {
    final GeoServerAddWorkspaceCommand cmd = new GeoServerAddWorkspaceCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerAddWorkspaceCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerRemoveDatastoreCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveDatastoreCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerRemoveDatastoreCommand cmd = new GeoServerRemoveDatastoreCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerRemoveDatastoreCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerRemoveWorkspaceCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveWorkspaceCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerRemoveWorkspaceCommand cmd = new GeoServerRemoveWorkspaceCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerRemoveWorkspaceCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerAddStyleCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerAddStyleCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerAddStyleCommand cmd = new GeoServerAddStyleCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerAddStyleCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerListDatastoresCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerListDatastoresCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerListDatastoresCommand cmd = new GeoServerListDatastoresCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerListDatastoresCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerListCoverageStoresCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoverageStoresCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerListCoverageStoresCommand cmd = new GeoServerListCoverageStoresCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerListCoverageStoresCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerAddLayerCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerAddLayerCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerAddLayerCommand cmd = new GeoServerAddLayerCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerAddLayerCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerListStylesCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerListStylesCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerListStylesCommand cmd = new GeoServerListStylesCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerListStylesCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerGetFeatureLayerCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerGetFeatureLayerCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerGetFeatureLayerCommand cmd = new GeoServerGetFeatureLayerCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerGetFeatureLayerCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerRemoveCoverageCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerRemoveCoverageCommand cmd = new GeoServerRemoveCoverageCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerRemoveCoverageCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerListCoveragesCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoveragesCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerListCoveragesCommand cmd = new GeoServerListCoveragesCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerListCoveragesCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerRemoveFeatureLayerCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveFeatureLayerCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerRemoveFeatureLayerCommand cmd = new GeoServerRemoveFeatureLayerCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerRemoveFeatureLayerCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerRemoveStyleCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveStyleCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerRemoveStyleCommand cmd = new GeoServerRemoveStyleCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerRemoveStyleCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerGetDatastoreCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerGetDatastoreCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerGetDatastoreCommand cmd = new GeoServerGetDatastoreCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerGetDatastoreCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerAddFeatureLayerCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerAddFeatureLayerCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerAddFeatureLayerCommand cmd = new GeoServerAddFeatureLayerCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerAddFeatureLayerCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void geoServerSetLayerStyleCommand(
      final org.locationtech.geowave.service.grpc.protobuf.GeoServerSetLayerStyleCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final GeoServerSetLayerStyleCommand cmd = new GeoServerSetLayerStyleCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing GeoServerSetLayerStyleCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }
}
