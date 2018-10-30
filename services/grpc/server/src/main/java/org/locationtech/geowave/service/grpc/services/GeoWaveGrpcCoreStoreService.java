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
import java.util.Map;

import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.store.cli.config.AddIndexGroupCommand;
import org.locationtech.geowave.core.store.cli.config.RemoveIndexCommand;
import org.locationtech.geowave.core.store.cli.config.RemoveIndexGroupCommand;
import org.locationtech.geowave.core.store.cli.config.RemoveStoreCommand;
import org.locationtech.geowave.core.store.cli.remote.CalculateStatCommand;
import org.locationtech.geowave.core.store.cli.remote.ClearCommand;
import org.locationtech.geowave.core.store.cli.remote.ListTypesCommand;
import org.locationtech.geowave.core.store.cli.remote.ListIndicesCommand;
import org.locationtech.geowave.core.store.cli.remote.ListStatsCommand;
import org.locationtech.geowave.core.store.cli.remote.RecalculateStatsCommand;
import org.locationtech.geowave.core.store.cli.remote.RemoveTypeCommand;
import org.locationtech.geowave.core.store.cli.remote.RemoveStatCommand;
import org.locationtech.geowave.core.store.operations.remote.VersionCommand;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CoreStoreGrpc.CoreStoreImplBase;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class GeoWaveGrpcCoreStoreService extends
		CoreStoreImplBase implements
		GeoWaveGrpcServiceSpi
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcCoreStoreService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void removeTypeCommand(
			org.locationtech.geowave.service.grpc.protobuf.RemoveTypeCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		RemoveTypeCommand cmd = new RemoveTypeCommand();
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

		LOGGER.info("Executing RemoveAdapterCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void removeStoreCommand(
			org.locationtech.geowave.service.grpc.protobuf.RemoveStoreCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		RemoveStoreCommand cmd = new RemoveStoreCommand();
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

		LOGGER.info("Executing RemoveStoreCommand...");
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
	public void listTypesCommand(
			org.locationtech.geowave.service.grpc.protobuf.ListTypesCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ListTypesCommand cmd = new ListTypesCommand();
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

		LOGGER.info("Executing ListAdapterCommand...");
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
	public void calculateStatCommand(
			org.locationtech.geowave.service.grpc.protobuf.CalculateStatCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		CalculateStatCommand cmd = new CalculateStatCommand();
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

		LOGGER.info("Executing CalculateStatCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void removeIndexGroupCommand(
			org.locationtech.geowave.service.grpc.protobuf.RemoveIndexGroupCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		RemoveIndexGroupCommand cmd = new RemoveIndexGroupCommand();
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

		LOGGER.info("Executing RemoveIndexGroupCommand...");
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
	public void recalculateStatsCommand(
			org.locationtech.geowave.service.grpc.protobuf.RecalculateStatsCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		RecalculateStatsCommand cmd = new RecalculateStatsCommand();
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

		LOGGER.info("Executing RecalculateStatsCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void listStatsCommand(
			org.locationtech.geowave.service.grpc.protobuf.ListStatsCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ListStatsCommand cmd = new ListStatsCommand();
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

		LOGGER.info("Executing ListStatsCommand...");
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
	public void listIndicesCommand(
			org.locationtech.geowave.service.grpc.protobuf.ListIndicesCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		ListIndicesCommand cmd = new ListIndicesCommand();
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

		LOGGER.info("Executing ListIndexCommand...");
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
	public void clearCommand(
			org.locationtech.geowave.service.grpc.protobuf.ClearCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {
		ClearCommand cmd = new ClearCommand();
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

		LOGGER.info("Executing ClearCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
	public void versionCommand(
			org.locationtech.geowave.service.grpc.protobuf.VersionCommandParameters request,
			io.grpc.stub.StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		VersionCommand cmd = new VersionCommand();
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

		LOGGER.info("Executing VersionCommand...");
		try {
			cmd.computeResults(params);
			final StringResponse resp = StringResponse.newBuilder().build();
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
	public void addIndexGroupCommand(
			org.locationtech.geowave.service.grpc.protobuf.AddIndexGroupCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		AddIndexGroupCommand cmd = new AddIndexGroupCommand();
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

		LOGGER.info("Executing AddIndexGroupCommand...");
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
	public void removeIndexCommand(
			org.locationtech.geowave.service.grpc.protobuf.RemoveIndexCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse> responseObserver ) {

		RemoveIndexCommand cmd = new RemoveIndexCommand();
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

		LOGGER.info("Executing RemoveIndexCommand...");
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
	public void removeStatCommand(
			org.locationtech.geowave.service.grpc.protobuf.RemoveStatCommandParameters request,
			StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse> responseObserver ) {

		RemoveStatCommand cmd = new RemoveStatCommand();
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

		LOGGER.info("Executing RemoveStatCommand...");
		try {
			cmd.computeResults(params);
			final VoidResponse resp = VoidResponse.newBuilder().build();
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
