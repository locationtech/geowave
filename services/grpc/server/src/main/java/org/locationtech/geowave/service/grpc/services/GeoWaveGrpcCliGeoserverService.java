/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.service.grpc.services;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.cli.geoserver.ConfigGeoServerCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerAddCoverageCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerAddCoverageStoreCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerAddDatastoreCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerAddFeatureLayerCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerAddLayerCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerAddStyleCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerAddWorkspaceCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerGetCoverageCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerGetCoverageStoreCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerGetDatastoreCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerGetFeatureLayerCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerGetStoreAdapterCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerGetStyleCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerListCoverageStoresCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerListCoveragesCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerListDatastoresCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerListFeatureLayersCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerListStylesCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerListWorkspacesCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveCoverageCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveCoverageStoreCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveDatastoreCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveFeatureLayerCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveStyleCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveWorkspaceCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerSetLayerStyleCommand;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc.CliGeoserverImplBase;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class GeoWaveGrpcCliGeoserverService extends
		CliGeoserverImplBase implements
		GeoWaveGrpcServiceSpi
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCliGeoserverService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void geoServerListWorkspacesCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerListWorkspacesCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse> responseObserver ) {

		GeoServerListWorkspacesCommand cmd = new GeoServerListWorkspacesCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerListWorkspacesCommand...");
		try {
			final List<String> result = cmd.computeResults(params);
			final RepeatedStringResponse resp = RepeatedStringResponse.newBuilder().addAllResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerAddCoverageCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddCoverageCommand cmd = new GeoServerAddCoverageCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerAddCoverageCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerRemoveCoverageStoreCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageStoreCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveCoverageStoreCommand cmd = new GeoServerRemoveCoverageStoreCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerRemoveCoverageStoreCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerAddCoverageStoreCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageStoreCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddCoverageStoreCommand cmd = new GeoServerAddCoverageStoreCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerAddCoverageStoreCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerGetCoverageStoreCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageStoreCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerGetCoverageStoreCommand cmd = new GeoServerGetCoverageStoreCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerGetCoverageStoreCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerAddDatastoreCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerAddDatastoreCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerAddDatastoreCommand cmd = new GeoServerAddDatastoreCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerAddDatastoreCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerGetStyleCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStyleCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerGetStyleCommand cmd = new GeoServerGetStyleCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerGetStyleCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void configGeoServerCommand(
			org.locationtech.geowave.service.grpc.protobuf.ConfigGeoServerCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ConfigGeoServerCommand cmd = new ConfigGeoServerCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing ConfigGeoServerCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerGetCoverageCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerGetCoverageCommand cmd = new GeoServerGetCoverageCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerGetCoverageCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerListFeatureLayersCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerListFeatureLayersCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListFeatureLayersCommand cmd = new GeoServerListFeatureLayersCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerListFeatureLayersCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerGetStoreAdapterCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStoreAdapterCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.RepeatedStringResponse> responseObserver ) {

		GeoServerGetStoreAdapterCommand cmd = new GeoServerGetStoreAdapterCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerGetStoreAdapterCommand...");
		try {
			final List<String> result = cmd.computeResults(params);
			final RepeatedStringResponse resp = RepeatedStringResponse.newBuilder().addAllResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerAddWorkspaceCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerAddWorkspaceCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {
		GeoServerAddWorkspaceCommand cmd = new GeoServerAddWorkspaceCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerAddWorkspaceCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerRemoveDatastoreCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveDatastoreCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveDatastoreCommand cmd = new GeoServerRemoveDatastoreCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerRemoveDatastoreCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerRemoveWorkspaceCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveWorkspaceCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveWorkspaceCommand cmd = new GeoServerRemoveWorkspaceCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerRemoveWorkspaceCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerAddStyleCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerAddStyleCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddStyleCommand cmd = new GeoServerAddStyleCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerAddStyleCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerListDatastoresCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerListDatastoresCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListDatastoresCommand cmd = new GeoServerListDatastoresCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerListDatastoresCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerListCoverageStoresCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoverageStoresCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListCoverageStoresCommand cmd = new GeoServerListCoverageStoresCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerListCoverageStoresCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerAddLayerCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerAddLayerCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddLayerCommand cmd = new GeoServerAddLayerCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerAddLayerCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerListStylesCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerListStylesCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListStylesCommand cmd = new GeoServerListStylesCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerListStylesCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerGetFeatureLayerCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerGetFeatureLayerCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerGetFeatureLayerCommand cmd = new GeoServerGetFeatureLayerCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerGetFeatureLayerCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerRemoveCoverageCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveCoverageCommand cmd = new GeoServerRemoveCoverageCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerRemoveCoverageCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerListCoveragesCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoveragesCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerListCoveragesCommand cmd = new GeoServerListCoveragesCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerListCoveragesCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerRemoveFeatureLayerCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveFeatureLayerCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveFeatureLayerCommand cmd = new GeoServerRemoveFeatureLayerCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerRemoveFeatureLayerCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerRemoveStyleCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveStyleCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerRemoveStyleCommand cmd = new GeoServerRemoveStyleCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerRemoveStyleCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

	@Override
	public void geoServerGetDatastoreCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerGetDatastoreCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerGetDatastoreCommand cmd = new GeoServerGetDatastoreCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerGetDatastoreCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerAddFeatureLayerCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerAddFeatureLayerCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerAddFeatureLayerCommand cmd = new GeoServerAddFeatureLayerCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerAddFeatureLayerCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void geoServerSetLayerStyleCommand(
			org.locationtech.geowave.service.grpc.protobuf.GeoServerSetLayerStyleCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		GeoServerSetLayerStyleCommand cmd = new GeoServerSetLayerStyleCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);

		LOGGER.info("Executing GeoServerSetLayerStyleCommand...");
		try {
			final String result = cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().setResponseValue(
					result).build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}

	}

}
