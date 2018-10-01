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
package org.locationtech.geowave.test.services.grpc;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.locationtech.geowave.core.geotime.GeometryUtils;
import org.locationtech.geowave.service.grpc.protobuf.AddIndexGroupCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticSparkGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CQLQueryParameters;
import org.locationtech.geowave.service.grpc.protobuf.CalculateStatCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ClearCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc;
import org.locationtech.geowave.service.grpc.protobuf.ConfigGeoServerCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ConfigHDFSCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.CoreCliGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreStoreGrpc;
import org.locationtech.geowave.service.grpc.protobuf.DBScanCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.Feature;
import org.locationtech.geowave.service.grpc.protobuf.FeatureAttribute;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageStoreCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddDatastoreCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddFeatureLayerCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddLayerCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddStyleCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddWorkspaceCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageStoreCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetDatastoreCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetFeatureLayerCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStoreAdapterCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStyleCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoverageStoresCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoveragesCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListDatastoresCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListFeatureLayersCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListStylesCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListWorkspacesCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageStoreCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveDatastoreCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveFeatureLayerCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveStyleCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveWorkspaceCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerSetLayerStyleCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.KafkaToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.KdeCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.KmeansSparkCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListAdapterCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListIndexCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListPluginsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListStatsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToHdfsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToKafkaCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToMapReduceToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.MapReduceToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.NearestNeighborCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RecalculateStatsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RemoveAdapterCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RemoveIndexCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RemoveIndexGroupCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RemoveStatCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RemoveStoreCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.SetCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.SparkSqlCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.SparkToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.SpatialJoinCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.SpatialQueryParameters;
import org.locationtech.geowave.service.grpc.protobuf.SpatialTemporalQueryParameters;
import org.locationtech.geowave.service.grpc.protobuf.TemporalConstraints;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc;
import org.locationtech.geowave.service.grpc.protobuf.VectorIngestParameters;
import org.locationtech.geowave.service.grpc.protobuf.VectorQueryParameters;
import org.locationtech.geowave.service.grpc.protobuf.VectorStoreParameters;
import org.locationtech.geowave.service.grpc.protobuf.VersionCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc.AnalyticMapreduceBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticSparkGrpc.AnalyticSparkBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc.CliGeoserverBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreCliGrpc.CoreCliBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc.CoreIngestBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreMapreduceGrpc.CoreMapreduceBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreStoreGrpc.CoreStoreBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.MapStringStringResponse;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc.VectorBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc.VectorStub;
import org.locationtech.geowave.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.vividsolutions.jts.geom.Coordinate;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcTestClient
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcTestClient.class.getName());

	public final ManagedChannel channel;
	public final VectorBlockingStub vectorBlockingStub;
	public final VectorStub vectorAsyncStub;
	public final CoreCliBlockingStub coreCliBlockingStub;
	public final CoreMapreduceBlockingStub coreMapreduceBlockingStub;
	public final AnalyticMapreduceBlockingStub analyticMapreduceBlockingStub;
	public final AnalyticSparkBlockingStub analyticSparkBlockingStub;
	public final CoreStoreBlockingStub coreStoreBlockingStub;
	public final CoreIngestBlockingStub coreIngestBlockingStub;
	public final CliGeoserverBlockingStub cliGeoserverBlockingStub;

	// test values
	public int numFeaturesProcessed = 0;

	public GeoWaveGrpcTestClient(
			String host,
			int port ) {
		this(
				NettyChannelBuilder.forAddress(
						host,
						port).nameResolverFactory(
						new DnsNameResolverProvider()).usePlaintext(
						true));
	}

	public GeoWaveGrpcTestClient(
			NettyChannelBuilder channelBuilder ) {
		channel = channelBuilder.build();
		vectorBlockingStub = VectorGrpc.newBlockingStub(channel);
		vectorAsyncStub = VectorGrpc.newStub(channel);
		coreCliBlockingStub = CoreCliGrpc.newBlockingStub(channel);
		coreMapreduceBlockingStub = CoreMapreduceGrpc.newBlockingStub(channel);
		analyticMapreduceBlockingStub = AnalyticMapreduceGrpc.newBlockingStub(channel);
		coreStoreBlockingStub = CoreStoreGrpc.newBlockingStub(channel);
		coreIngestBlockingStub = CoreIngestGrpc.newBlockingStub(channel);
		cliGeoserverBlockingStub = CliGeoserverGrpc.newBlockingStub(channel);
		analyticSparkBlockingStub = AnalyticSparkGrpc.newBlockingStub(channel);
	}

	public void shutdown()
			throws InterruptedException {
		channel.shutdown().awaitTermination(
				5,
				TimeUnit.SECONDS);
	}

	// Core CLI methods
	public void setCommand(
			final String key,
			final String val ) {
		ArrayList<String> params = new ArrayList<String>();
		params.add(key);
		params.add(val);
		SetCommandParameters request = SetCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreCliBlockingStub.setCommand(request);
	}

	public Map<String, String> listCommand() {
		ListCommandParameters request = ListCommandParameters.newBuilder().build();
		MapStringStringResponse response = coreCliBlockingStub.listCommand(request);
		Map<String, String> map = response.getResponseValueMap();
		return map;
	}

	// Vector Service Methods
	public void vectorIngest(
			int minLat,
			int maxLat,
			int minLon,
			int maxLon,
			int latStepDegs,
			int lonStepDegs )
			throws InterruptedException,
			UnsupportedEncodingException {
		LOGGER.info("Performing Vector Ingest...");
		VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setAdapterId(
				copyFrom(GeoWaveGrpcTestUtils.adapterId.getBytes("UTF-8"))).setIndexId(
				copyFrom(GeoWaveGrpcTestUtils.indexId.getBytes("UTF-8"))).build();

		final CountDownLatch finishLatch = new CountDownLatch(
				1);
		StreamObserver<StringResponse> responseObserver = new StreamObserver<StringResponse>() {

			@Override
			public void onNext(
					StringResponse value ) {
				try {
					numFeaturesProcessed = Integer.parseInt(value.getResponseValue());
				}
				catch (final NumberFormatException e) {

				}
				LOGGER.info(value.getResponseValue());
			}

			@Override
			public void onError(
					Throwable t ) {
				LOGGER.error(
						"Error: Vector Ingest failed.",
						t);
				finishLatch.countDown();
			}

			@Override
			public void onCompleted() {
				LOGGER.info("Finished Vector Ingest...");
				finishLatch.countDown();
			}
		};
		StreamObserver<VectorIngestParameters> requestObserver = vectorAsyncStub.vectorIngest(responseObserver);

		// Build up and add features to the request here...
		VectorIngestParameters.Builder requestBuilder = VectorIngestParameters.newBuilder();
		FeatureAttribute.Builder attBuilder = FeatureAttribute.newBuilder();
		for (int longitude = minLon; longitude <= maxLon; longitude += lonStepDegs) {
			for (int latitude = minLat; latitude <= maxLat; latitude += latStepDegs) {
				attBuilder.setValGeometry(GeometryUtils.GEOMETRY_FACTORY.createPoint(
						new Coordinate(
								longitude,
								latitude)).toString());
				requestBuilder.putFeature(
						"geometry",
						attBuilder.build());

				TimeZone tz = TimeZone.getTimeZone("UTC");
				DateFormat df = new SimpleDateFormat(
						"yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC,
													// no timezone offset
				df.setTimeZone(tz);
				String isoDate = df.format(new Date());
				attBuilder.setValString(isoDate);
				requestBuilder.putFeature(
						"TimeStamp",
						attBuilder.build());

				attBuilder.setValDouble(latitude);
				requestBuilder.putFeature(
						"Latitude",
						attBuilder.build());

				attBuilder.setValDouble(longitude);
				requestBuilder.putFeature(
						"Longitude",
						attBuilder.build());

				VectorIngestParameters params = requestBuilder.setBaseParams(
						baseParams).build();
				requestObserver.onNext(params);
				if (finishLatch.getCount() == 0) {
					// RPC completed or errored before we finished sending.
					// Sending further requests won't error, but they will just
					// be thrown away.
					return;
				}

			}
		}
		// Mark the end of requests
		requestObserver.onCompleted();

		// Receiving happens asynchronously
		if (!finishLatch.await(
				15,
				TimeUnit.MINUTES)) {
			LOGGER.warn("Vector Ingest can not finish within 5 minutes");
		}
	}

	public ArrayList<Feature> vectorQuery()
			throws UnsupportedEncodingException {
		LOGGER.info("Performing Vector Query...");
		VectorQueryParameters request = VectorQueryParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setAdapterId(
				copyFrom(GeoWaveGrpcTestUtils.adapterId.getBytes("UTF-8"))).setQuery(
				GeoWaveGrpcTestUtils.cqlSpatialQuery).build();

		Iterator<Feature> features = vectorBlockingStub.vectorQuery(request);
		final ArrayList<Feature> feature_list = new ArrayList<Feature>();

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	private static ByteString copyFrom(
			byte[] bytes ) {
		return ByteString.copyFrom(bytes);
	}

	public ArrayList<Feature> cqlQuery()
			throws UnsupportedEncodingException {
		LOGGER.info("Performing CQL Query...");
		VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setAdapterId(
				copyFrom(GeoWaveGrpcTestUtils.adapterId.getBytes("UTF-8"))).setIndexId(
				copyFrom(GeoWaveGrpcTestUtils.indexId.getBytes("UTF-8"))).build();

		CQLQueryParameters request = CQLQueryParameters.newBuilder().setBaseParams(
				baseParams).setCql(
				GeoWaveGrpcTestUtils.cqlSpatialQuery).build();

		Iterator<Feature> features;
		final ArrayList<Feature> feature_list = new ArrayList<Feature>();
		features = vectorBlockingStub.cqlQuery(request);

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	public ArrayList<Feature> spatialQuery()
			throws UnsupportedEncodingException {
		LOGGER.info("Performing Spatial Query...");
		VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setAdapterId(
				copyFrom(GeoWaveGrpcTestUtils.adapterId.getBytes("UTF-8"))).setIndexId(
				copyFrom(GeoWaveGrpcTestUtils.indexId.getBytes("UTF-8"))).build();

		final String queryPolygonDefinition = GeoWaveGrpcTestUtils.wktSpatialQuery;
		SpatialQueryParameters request = SpatialQueryParameters.newBuilder().setBaseParams(
				baseParams).setGeometry(
				queryPolygonDefinition).build();

		Iterator<Feature> features;
		final ArrayList<Feature> feature_list = new ArrayList<Feature>();
		features = vectorBlockingStub.spatialQuery(request);

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	public ArrayList<Feature> spatialTemporalQuery()
			throws ParseException {
		LOGGER.info("Performing Spatial Temporal Query...");
		VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).build();

		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC,
											// no timezone offset
		df.setTimeZone(tz);
		final String queryPolygonDefinition = GeoWaveGrpcTestUtils.wktSpatialQuery;

		SpatialQueryParameters spatialQuery = SpatialQueryParameters.newBuilder().setBaseParams(
				baseParams).setGeometry(
				queryPolygonDefinition).build();
		TemporalConstraints t = TemporalConstraints.newBuilder().setStartTime(
				Timestamps.fromMillis(df.parse(
						GeoWaveGrpcTestUtils.temporalQueryStartTime).getTime())).setEndTime(
				Timestamps.fromMillis(df.parse(
						GeoWaveGrpcTestUtils.temporalQueryEndTime).getTime())).build();
		SpatialTemporalQueryParameters request = SpatialTemporalQueryParameters.newBuilder().setSpatialParams(
				spatialQuery).addTemporalConstraints(
				0,
				t).setCompareOperation(
				"CONTAINS").build();

		Iterator<Feature> features;
		final ArrayList<Feature> feature_list = new ArrayList<Feature>();
		features = vectorBlockingStub.spatialTemporalQuery(request);

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	// Core Mapreduce
	public boolean configHDFSCommand() {
		ConfigHDFSCommandParameters request = ConfigHDFSCommandParameters.newBuilder().addParameters(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).build();
		coreMapreduceBlockingStub.configHDFSCommand(request);
		return true;
	}

	// Analytic Mapreduce
	public boolean dbScanCommand() {
		ArrayList<String> adapters = new ArrayList<String>();
		adapters.add(GeoWaveGrpcTestUtils.adapterId);
		DBScanCommandParameters request = DBScanCommandParameters.newBuilder().addParameters(
				GeoWaveGrpcTestUtils.storeName).setClusteringMaxIterations(
				"5").setClusteringMinimumSize(
				"10").setExtractMinInputSplit(
				"2").setExtractMaxInputSplit(
				"6").setPartitionMaxDistance(
				"1000").setOutputReducerCount(
				"4").setMapReduceHdfsHostPort(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).setMapReduceJobtrackerHostPort(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker()).setMapReduceHdfsBaseDir(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory()).addAllAdapterIds(
				adapters).build();
		analyticMapreduceBlockingStub.dBScanCommand(request);
		return true;
	}

	public boolean nearestNeighborCommand() {
		ArrayList<String> adapters = new ArrayList<String>();
		adapters.add(GeoWaveGrpcTestUtils.adapterId);
		NearestNeighborCommandParameters request = NearestNeighborCommandParameters
				.newBuilder()
				.addParameters(
						GeoWaveGrpcTestUtils.storeName)
				.addAllAdapterIds(
						adapters)
				.setExtractQuery(
						GeoWaveGrpcTestUtils.wktSpatialQuery)
				.setExtractMinInputSplit(
						"2")
				.setExtractMaxInputSplit(
						"6")
				.setPartitionMaxDistance(
						"10")
				.setOutputReducerCount(
						"4")
				.setMapReduceHdfsHostPort(
						GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs())
				.setMapReduceJobtrackerHostPort(
						GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker())
				.setOutputHdfsOutputPath(
						GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory() + "/GrpcNearestNeighbor")
				.setMapReduceHdfsBaseDir(
						GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory())
				.build();
		analyticMapreduceBlockingStub.nearestNeighborCommand(request);
		return true;
	}

	public boolean kdeCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.outputStoreName);
		KdeCommandParameters request = KdeCommandParameters.newBuilder().addAllParameters(
				params).setCoverageName(
				"grpc_kde").setFeatureType(
				GeoWaveGrpcTestUtils.adapterId).setHdfsHostPort(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).setJobTrackerOrResourceManHostPort(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker()).setMinLevel(
				5).setMaxLevel(
				26).setMinSplits(
				32).setMaxSplits(
				32).setTileSize(
				1).build();
		analyticMapreduceBlockingStub.kdeCommand(request);
		return true;
	}

	// Core Store
	public boolean RecalculateStatsCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		RecalculateStatsCommandParameters request = RecalculateStatsCommandParameters.newBuilder().addAllParameters(
				params).setJsonFormatFlag(
				true).build();
		coreStoreBlockingStub.recalculateStatsCommand(request);
		return true;
	}

	public String RemoveIndexCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.indexId);
		RemoveIndexCommandParameters request = RemoveIndexCommandParameters.newBuilder().addAllParameters(
				params).build();
		StringResponse resp = coreStoreBlockingStub.removeIndexCommand(request);
		return resp.getResponseValue();
	}

	public boolean VersionCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		VersionCommandParameters request = VersionCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.versionCommand(request);
		return true;
	}

	public String ListIndexCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		ListIndexCommandParameters request = ListIndexCommandParameters.newBuilder().addAllParameters(
				params).build();
		StringResponse resp = coreStoreBlockingStub.listIndexCommand(request);
		return resp.getResponseValue();
	}

	public String ListStatsCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		ListStatsCommandParameters request = ListStatsCommandParameters.newBuilder().addAllParameters(
				params).build();
		StringResponse resp = coreStoreBlockingStub.listStatsCommand(request);
		return resp.getResponseValue();
	}

	public String AddIndexGroupCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.indexId + "-group");
		params.add(GeoWaveGrpcTestUtils.indexId);
		AddIndexGroupCommandParameters request = AddIndexGroupCommandParameters.newBuilder().addAllParameters(
				params).build();
		StringResponse resp = coreStoreBlockingStub.addIndexGroupCommand(request);
		return resp.getResponseValue();
	}

	public boolean ClearCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		ClearCommandParameters request = ClearCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.clearCommand(request);
		return true;
	}

	public String ListAdapterCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		ListAdapterCommandParameters request = ListAdapterCommandParameters.newBuilder().addAllParameters(
				params).build();
		StringResponse resp = coreStoreBlockingStub.listAdapterCommand(request);
		return resp.getResponseValue();
	}

	public String RemoveStoreCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		RemoveStoreCommandParameters request = RemoveStoreCommandParameters.newBuilder().addAllParameters(
				params).build();
		StringResponse resp = coreStoreBlockingStub.removeStoreCommand(request);
		return resp.getResponseValue();
	}

	public boolean RemoveAdapterCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.adapterId);
		RemoveAdapterCommandParameters request = RemoveAdapterCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.removeAdapterCommand(request);
		return true;
	}

	public boolean RemoveStatCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.adapterId);
		params.add("FEATURE_BBOX#geometry");
		RemoveStatCommandParameters request = RemoveStatCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.removeStatCommand(request);
		return true;
	}

	public boolean CalculateStatCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.adapterId);
		params.add("FEATURE_BBOX#geometry");
		CalculateStatCommandParameters request = CalculateStatCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.calculateStatCommand(request);
		return true;
	}

	public String RemoveIndexGroupCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.indexId + "-group");
		RemoveIndexGroupCommandParameters request = RemoveIndexGroupCommandParameters.newBuilder().addAllParameters(
				params).build();
		StringResponse resp = coreStoreBlockingStub.removeIndexGroupCommand(request);
		return resp.getResponseValue();
	}

	// Cli GeoServer
	public String GeoServerAddLayerCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerAddLayerCommandParameters request = GeoServerAddLayerCommandParameters.newBuilder().addAllParameters(
				params).setAdapterId(
				"GeometryTest").setAddOption(
				"VECTOR").setStyle(
				"default").setWorkspace(
				"default").build();
		return cliGeoserverBlockingStub.geoServerAddLayerCommand(
				request).getResponseValue();
	}

	public String GeoServerGetDatastoreCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerGetDatastoreCommandParameters request = GeoServerGetDatastoreCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerGetDatastoreCommand(
				request).getResponseValue();
	}

	public String GeoServerGetFeatureLayerCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerGetFeatureLayerCommandParameters request = GeoServerGetFeatureLayerCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerGetFeatureLayerCommand(
				request).getResponseValue();
	}

	public String GeoServerListCoverageStoresCommand() {
		GeoServerListCoverageStoresCommandParameters request = GeoServerListCoverageStoresCommandParameters
				.newBuilder()
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerListCoverageStoresCommand(
				request).getResponseValue();
	}

	public List<String> GeoServerGetStoreAdapterCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerGetStoreAdapterCommandParameters request = GeoServerGetStoreAdapterCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerGetStoreAdapterCommand(
				request).getResponseValueList();
	}

	public String GeoServerGetCoverageCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerGetCoverageCommandParameters request = GeoServerGetCoverageCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.setCvgstore(
						"test_cvg_store")
				.build();
		return cliGeoserverBlockingStub.geoServerGetCoverageCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveFeatureLayerCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerRemoveFeatureLayerCommandParameters request = GeoServerRemoveFeatureLayerCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveFeatureLayerCommand(
				request).getResponseValue();
	}

	public String GeoServerAddCoverageCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerAddCoverageCommandParameters request = GeoServerAddCoverageCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.setCvgstore(
						"test_cvg_store")
				.build();
		return cliGeoserverBlockingStub.geoServerAddCoverageCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveWorkspaceCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerRemoveWorkspaceCommandParameters request = GeoServerRemoveWorkspaceCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveWorkspaceCommand(
				request).getResponseValue();
	}

	public List<String> GeoServerListWorkspacesCommand() {
		GeoServerListWorkspacesCommandParameters request = GeoServerListWorkspacesCommandParameters
				.newBuilder()
				.build();
		return cliGeoserverBlockingStub.geoServerListWorkspacesCommand(
				request).getResponseValueList();
	}

	public String GeoServerGetCoverageStoreCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerGetCoverageStoreCommandParameters request = GeoServerGetCoverageStoreCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerGetCoverageStoreCommand(
				request).getResponseValue();
	}

	public String ConfigGeoServerCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		ConfigGeoServerCommandParameters request = ConfigGeoServerCommandParameters.newBuilder().addAllParameters(
				params).setWorkspace(
				"default").setUsername(
				"user").setPass(
				"default").build();
		return cliGeoserverBlockingStub.configGeoServerCommand(
				request).getResponseValue();
	}

	public String GeoServerListCoveragesCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerListCoveragesCommandParameters request = GeoServerListCoveragesCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerListCoveragesCommand(
				request).getResponseValue();
	}

	public String GeoServerListStylesCommand() {
		GeoServerListStylesCommandParameters request = GeoServerListStylesCommandParameters.newBuilder().build();
		return cliGeoserverBlockingStub.geoServerListStylesCommand(
				request).getResponseValue();
	}

	public String GeoServerAddCoverageStoreCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerAddCoverageStoreCommandParameters request = GeoServerAddCoverageStoreCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.setCoverageStore(
						"coverage-store")
				.setEqualizeHistogramOverride(
						false)
				.setScaleTo8Bit(
						false)
				.setInterpolationOverride(
						"0")
				.build();
		return cliGeoserverBlockingStub.geoServerAddCoverageStoreCommand(
				request).getResponseValue();
	}

	public String GeoServerAddFeatureLayerCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerAddFeatureLayerCommandParameters request = GeoServerAddFeatureLayerCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.setDatastore(
						"grpc")
				.build();
		return cliGeoserverBlockingStub.geoServerAddFeatureLayerCommand(
				request).getResponseValue();
	}

	public String GeoServerAddDatastoreCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		GeoServerAddDatastoreCommandParameters request = GeoServerAddDatastoreCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.setDatastore(
						"grpc-store")
				.build();
		return cliGeoserverBlockingStub.geoServerAddDatastoreCommand(
				request).getResponseValue();
	}

	public String GeoServerListDatastoresCommand() {
		GeoServerListDatastoresCommandParameters request = GeoServerListDatastoresCommandParameters
				.newBuilder()
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerListDatastoresCommand(
				request).getResponseValue();
	}

	public String GeoServerSetLayerStyleCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerSetLayerStyleCommandParameters request = GeoServerSetLayerStyleCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setStyleName(
						"test-style")
				.build();
		return cliGeoserverBlockingStub.geoServerSetLayerStyleCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveCoverageStoreCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerRemoveCoverageStoreCommandParameters request = GeoServerRemoveCoverageStoreCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveCoverageStoreCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveDatastoreCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerRemoveDatastoreCommandParameters request = GeoServerRemoveDatastoreCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveDatastoreCommand(
				request).getResponseValue();
	}

	public String GeoServerAddStyleCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerAddStyleCommandParameters request = GeoServerAddStyleCommandParameters.newBuilder().addAllParameters(
				params).setStylesld(
				"styles-id").build();
		return cliGeoserverBlockingStub.geoServerAddStyleCommand(
				request).getResponseValue();
	}

	public String GeoServerAddWorkspaceCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerAddWorkspaceCommandParameters request = GeoServerAddWorkspaceCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerAddWorkspaceCommand(
				request).getResponseValue();
	}

	public String GeoServerGetStyleCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerGetStyleCommandParameters request = GeoServerGetStyleCommandParameters.newBuilder().addAllParameters(
				params).build();
		return cliGeoserverBlockingStub.geoServerGetStyleCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveStyleCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerRemoveStyleCommandParameters request = GeoServerRemoveStyleCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveStyleCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveCoverageCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("grpc");
		GeoServerRemoveCoverageCommandParameters request = GeoServerRemoveCoverageCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.setCvgstore(
						"cvg-store")
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveCoverageCommand(
				request).getResponseValue();
	}

	public String GeoServerListFeatureLayersCommand() {
		GeoServerListFeatureLayersCommandParameters request = GeoServerListFeatureLayersCommandParameters
				.newBuilder()
				.setWorkspace(
						"default")
				.setDatastore(
						"cvg-store")
				.setGeowaveOnly(
						true)
				.build();
		return cliGeoserverBlockingStub.geoServerListFeatureLayersCommand(
				request).getResponseValue();
	}

	// Core Ingest
	public boolean LocalToHdfsCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());

		ArrayList<String> extensions = new ArrayList<String>();

		LocalToHdfsCommandParameters request = LocalToHdfsCommandParameters.newBuilder().addAllParameters(
				params).addAllExtensions(
				extensions).setFormats(
				"gpx").build();
		coreIngestBlockingStub.localToHdfsCommand(request);
		return true;
	}

	public boolean LocalToGeowaveCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexId);

		ArrayList<String> extensions = new ArrayList<String>();

		LocalToGeowaveCommandParameters request = LocalToGeowaveCommandParameters.newBuilder().addAllParameters(
				params).addAllExtensions(
				extensions).setFormats(
				"gpx").setThreads(
				1).build();
		coreIngestBlockingStub.localToGeowaveCommand(request);
		return true;
	}

	public boolean MapReduceToGeowaveCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexId);

		ArrayList<String> extensions = new ArrayList<String>();
		MapReduceToGeowaveCommandParameters request = MapReduceToGeowaveCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.addAllExtensions(
						extensions)
				.setFormats(
						"gpx")
				.setJobTrackerHostPort(
						GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker())
				.build();
		coreIngestBlockingStub.mapReduceToGeowaveCommand(request);
		return true;
	}

	public boolean SparkToGeowaveCommand() {
		ArrayList<String> params = new ArrayList<String>();

		final File tempDataDir = new File(
				"./" + TestUtils.TEST_CASE_BASE);
		String hdfsPath = "";
		try {
			hdfsPath = tempDataDir.toURI().toURL().toString();
		}
		catch (MalformedURLException e) {
			return false;
		}

		// uncomment this line and comment-out the following to test s3 vs hdfs
		// params.add("s3://geowave-test/data/gdelt");
		params.add(hdfsPath + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexId);

		ArrayList<String> extensions = new ArrayList<String>();

		SparkToGeowaveCommandParameters request = SparkToGeowaveCommandParameters.newBuilder().addAllParameters(
				params).addAllExtensions(
				extensions).setFormats(
				"gpx").setAppName(
				"CoreGeoWaveSparkITs").setMaster(
				"local[*]").setHost(
				"localhost").setNumExecutors(
				1).setNumCores(
				1).build();
		coreIngestBlockingStub.sparkToGeowaveCommand(request);
		return true;
	}

	public boolean LocalToMapReduceToGeowaveCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexId);

		ArrayList<String> extensions = new ArrayList<String>();

		LocalToMapReduceToGeowaveCommandParameters request = LocalToMapReduceToGeowaveCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.addAllExtensions(
						extensions)
				.setFormats(
						"gpx")
				.setJobTrackerHostPort(
						GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker())
				.build();
		coreIngestBlockingStub.localToMapReduceToGeowaveCommand(request);
		return true;
	}

	public boolean KafkaToGeowaveCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexId);

		ArrayList<String> extensions = new ArrayList<String>();

		KafkaToGeowaveCommandParameters request = KafkaToGeowaveCommandParameters.newBuilder().addAllParameters(
				params).addAllExtensions(
				extensions).setFormats(
				"gpx").setGroupId(
				"testGroup").setZookeeperConnect(
				GeoWaveGrpcTestUtils.getZookeeperTestEnv().getZookeeper()).setAutoOffsetReset(
				"smallest").setFetchMessageMaxBytes(
				"5000000").setConsumerTimeoutMs(
				"5000").setReconnectOnTimeout(
				false).setBatchSize(
				10000).build();
		coreIngestBlockingStub.kafkaToGeowaveCommand(request);
		return true;
	}

	public String ListPluginsCommand() {
		ListPluginsCommandParameters request = ListPluginsCommandParameters.newBuilder().build();
		return coreIngestBlockingStub.listPluginsCommand(
				request).getResponseValue();
	}

	public boolean LocalToKafkaCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");

		ArrayList<String> extensions = new ArrayList<String>();

		String localhost = "localhost";
		try {
			localhost = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		}
		catch (final UnknownHostException e) {
			LOGGER.warn(
					"unable to get canonical hostname for localhost",
					e);
		}

		LocalToKafkaCommandParameters request = LocalToKafkaCommandParameters.newBuilder().addAllParameters(
				params).addAllExtensions(
				extensions).setFormats(
				"gpx").setMetadataBrokerList(
				localhost + ":9092").setRequestRequiredAcks(
				"1").setProducerType(
				"sync").setSerializerClass(
				"org.locationtech.geowave.core.ingest.kafka.AvroKafkaEncoder").setRetryBackoffMs(
				"1000").build();
		coreIngestBlockingStub.localToKafkaCommand(request);
		return true;
	}

	// Analytic Spark
	public boolean KmeansSparkCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.outputStoreName);
		KmeansSparkCommandParameters request = KmeansSparkCommandParameters.newBuilder().addAllParameters(
				params).setAppName(
				"test-app") // Spark app name
				.setHost(
						"localhost")
				// spark host
				.setMaster(
						"local[*]")
				// spark master designation Id
				.setAdapterId(
						GeoWaveGrpcTestUtils.adapterId)
				.setNumClusters(
						2)
				//
				.setNumIterations(
						2)
				.setEpsilon(
						20.0)
				.setUseTime(
						false)
				.setGenerateHulls(
						true)
				// optional
				.setComputeHullData(
						true)
				// optional
				.setCqlFilter(
						GeoWaveGrpcTestUtils.cqlSpatialQuery)
				.setMinSplits(
						1)
				.setMaxSplits(
						4)
				.setCentroidTypeName(
						"poly")
				.setHullTypeName(
						"poly-hull")
				.build();
		analyticSparkBlockingStub.kmeansSparkCommand(request);
		return true;
	}

	public boolean SparkSqlCommand() {
		ArrayList<String> params = new ArrayList<String>();
		params.add("select * from %" + GeoWaveGrpcTestUtils.storeName + "|" + GeoWaveGrpcTestUtils.adapterId);
		SparkSqlCommandParameters request = SparkSqlCommandParameters.newBuilder().addAllParameters(
				params).setOutputStoreName(
				GeoWaveGrpcTestUtils.outputStoreName).setMaster(
				"local[*]").setAppName(
				"sparkSqlTestApp").setHost(
				"localhost").setOutputTypeName(
				GeoWaveGrpcTestUtils.adapterId).setShowResults(
				5).build();
		analyticSparkBlockingStub.sparkSqlCommand(request);
		return true;
	}

	public boolean SpatialJoinCommand() {

		ArrayList<String> params = new ArrayList<String>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.outputStoreName);
		SpatialJoinCommandParameters request = SpatialJoinCommandParameters.newBuilder().addAllParameters(
				params).setAppName(
				"test-app2").setMaster(
				"local[*]").setHost(
				"localhost").setLeftAdapterId(
				GeoWaveGrpcTestUtils.adapterId).setRightAdapterId(
				GeoWaveGrpcTestUtils.adapterId).setOutLeftAdapterId(
				GeoWaveGrpcTestUtils.adapterId + "_l").setOutRightAdapterId(
				GeoWaveGrpcTestUtils.adapterId + "_r").setPredicate(
				"GeomIntersects").setRadius(
				0.1).setNegativeTest(
				false).build();
		analyticSparkBlockingStub.spatialJoinCommand(request);
		return true;
	}

}
