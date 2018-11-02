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

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.service.grpc.protobuf.AddIndexGroupCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc.AnalyticMapreduceBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticSparkGrpc;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticSparkGrpc.AnalyticSparkBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CQLQueryParameters;
import org.locationtech.geowave.service.grpc.protobuf.CalculateStatCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ClearCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc.CliGeoserverBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.ConfigGeoServerCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ConfigHDFSCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.CoreCliGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreCliGrpc.CoreCliBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc.CoreIngestBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreMapreduceGrpc.CoreMapreduceBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreStoreGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreStoreGrpc.CoreStoreBlockingStub;
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
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.MapStringStringResponse;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.StringResponse;
import org.locationtech.geowave.service.grpc.protobuf.KafkaToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.KdeCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.KmeansSparkCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListTypesCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListIndicesCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListPluginsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.ListStatsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToHdfsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToKafkaCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.LocalToMapReduceToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.MapReduceToGeowaveCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.NearestNeighborCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RecalculateStatsCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.RemoveTypeCommandParameters;
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
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc.VectorBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc.VectorStub;
import org.locationtech.geowave.service.grpc.protobuf.VectorIngestParameters;
import org.locationtech.geowave.service.grpc.protobuf.VectorQueryParameters;
import org.locationtech.geowave.service.grpc.protobuf.VectorStoreParameters;
import org.locationtech.geowave.service.grpc.protobuf.VersionCommandParameters;
import org.locationtech.geowave.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.io.WKBWriter;

import io.grpc.ManagedChannel;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.NettyChannelBuilder;
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
			final String host,
			final int port ) {
		this(
				NettyChannelBuilder.forAddress(
						host,
						port).nameResolverFactory(
						new DnsNameResolverProvider()).usePlaintext(
						true));
	}

	public GeoWaveGrpcTestClient(
			final NettyChannelBuilder channelBuilder ) {
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(key);
		params.add(val);
		final SetCommandParameters request = SetCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreCliBlockingStub.setCommand(request);
	}

	public Map<String, String> listCommand() {
		final ListCommandParameters request = ListCommandParameters.newBuilder().build();
		final MapStringStringResponse response = coreCliBlockingStub.listCommand(request);
		final Map<String, String> map = response.getResponseValueMap();
		return map;
	}

	// Vector Service Methods
	public void vectorIngest(
			final int minLat,
			final int maxLat,
			final int minLon,
			final int maxLon,
			final int latStepDegs,
			final int lonStepDegs )
			throws InterruptedException,
			UnsupportedEncodingException {
		LOGGER.info("Performing Vector Ingest...");
		final VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setTypeName(
				GeoWaveGrpcTestUtils.typeName).setIndexName(
				GeoWaveGrpcTestUtils.indexName).build();

		final CountDownLatch finishLatch = new CountDownLatch(
				1);
		final StreamObserver<StringResponse> responseObserver = new StreamObserver<StringResponse>() {

			@Override
			public void onNext(
					final StringResponse value ) {
				try {
					numFeaturesProcessed = Integer.parseInt(value.getResponseValue());
				}
				catch (final NumberFormatException e) {

				}
				LOGGER.info(value.getResponseValue());
			}

			@Override
			public void onError(
					final Throwable t ) {
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
		final StreamObserver<VectorIngestParameters> requestObserver = vectorAsyncStub.vectorIngest(responseObserver);

		// Build up and add features to the request here...
		final VectorIngestParameters.Builder requestBuilder = VectorIngestParameters.newBuilder();
		final FeatureAttribute.Builder attBuilder = FeatureAttribute.newBuilder();
		for (int longitude = minLon; longitude <= maxLon; longitude += lonStepDegs) {
			for (int latitude = minLat; latitude <= maxLat; latitude += latStepDegs) {
				attBuilder.setValGeometry(copyFrom(new WKBWriter().write(GeometryUtils.GEOMETRY_FACTORY
						.createPoint(new Coordinate(
								longitude,
								latitude)))));
				requestBuilder.putFeature(
						"geometry",
						attBuilder.build());

				final TimeZone tz = TimeZone.getTimeZone("UTC");
				final DateFormat df = new SimpleDateFormat(
						"yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC,
													// no timezone offset
				df.setTimeZone(tz);
				final String isoDate = df.format(new Date());
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

				final VectorIngestParameters params = requestBuilder.setBaseParams(
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
		final VectorQueryParameters request = VectorQueryParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setTypeName(
				GeoWaveGrpcTestUtils.typeName).setQuery(
				GeoWaveGrpcTestUtils.cqlSpatialQuery).build();

		final Iterator<Feature> features = vectorBlockingStub.vectorQuery(request);
		final ArrayList<Feature> feature_list = new ArrayList<>();

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			final Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	private static ByteString copyFrom(
			final byte[] bytes ) {
		return ByteString.copyFrom(bytes);
	}

	public ArrayList<Feature> cqlQuery()
			throws UnsupportedEncodingException {
		LOGGER.info("Performing CQL Query...");
		final VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setTypeName(
				GeoWaveGrpcTestUtils.typeName).setIndexName(
				GeoWaveGrpcTestUtils.indexName).build();

		final CQLQueryParameters request = CQLQueryParameters.newBuilder().setBaseParams(
				baseParams).setCql(
				GeoWaveGrpcTestUtils.cqlSpatialQuery).build();

		Iterator<Feature> features;
		final ArrayList<Feature> feature_list = new ArrayList<>();
		features = vectorBlockingStub.cqlQuery(request);

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			final Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	public ArrayList<Feature> spatialQuery()
			throws UnsupportedEncodingException {
		LOGGER.info("Performing Spatial Query...");
		final VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).setTypeName(
				GeoWaveGrpcTestUtils.typeName).setIndexName(
				GeoWaveGrpcTestUtils.indexName).build();

		final SpatialQueryParameters request = SpatialQueryParameters.newBuilder().setBaseParams(
				baseParams).setGeometry(
				copyFrom(GeoWaveGrpcTestUtils.wkbSpatialQuery)).build();

		Iterator<Feature> features;
		final ArrayList<Feature> feature_list = new ArrayList<>();
		features = vectorBlockingStub.spatialQuery(request);

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			final Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	public ArrayList<Feature> spatialTemporalQuery()
			throws ParseException {
		LOGGER.info("Performing Spatial Temporal Query...");
		final VectorStoreParameters baseParams = VectorStoreParameters.newBuilder().setStoreName(
				GeoWaveGrpcTestUtils.storeName).build();

		final TimeZone tz = TimeZone.getTimeZone("UTC");
		final DateFormat df = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC,
											// no timezone offset
		df.setTimeZone(tz);

		final SpatialQueryParameters spatialQuery = SpatialQueryParameters.newBuilder().setBaseParams(
				baseParams).setGeometry(
				copyFrom(GeoWaveGrpcTestUtils.wkbSpatialQuery)).build();
		final TemporalConstraints t = TemporalConstraints.newBuilder().setStartTime(
				Timestamps.fromMillis(df.parse(
						GeoWaveGrpcTestUtils.temporalQueryStartTime).getTime())).setEndTime(
				Timestamps.fromMillis(df.parse(
						GeoWaveGrpcTestUtils.temporalQueryEndTime).getTime())).build();
		final SpatialTemporalQueryParameters request = SpatialTemporalQueryParameters.newBuilder().setSpatialParams(
				spatialQuery).addTemporalConstraints(
				0,
				t).setCompareOperation(
				"CONTAINS").build();

		Iterator<Feature> features;
		final ArrayList<Feature> feature_list = new ArrayList<>();
		features = vectorBlockingStub.spatialTemporalQuery(request);

		// iterate over features
		for (int i = 1; features.hasNext(); i++) {
			final Feature feature = features.next();
			feature_list.add(feature);
		}
		return feature_list;
	}

	// Core Mapreduce
	public boolean configHDFSCommand() {
		final ConfigHDFSCommandParameters request = ConfigHDFSCommandParameters.newBuilder().addParameters(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).build();
		coreMapreduceBlockingStub.configHDFSCommand(request);
		return true;
	}

	// Analytic Mapreduce
	public boolean dbScanCommand() {
		final ArrayList<String> types = new ArrayList<>();
		types.add(GeoWaveGrpcTestUtils.typeName);
		final DBScanCommandParameters request = DBScanCommandParameters.newBuilder().addParameters(
				GeoWaveGrpcTestUtils.storeName).setClusteringMaxIterations(
				"5").setClusteringMinimumSize(
				"10").setExtractMinInputSplit(
				"2").setExtractMaxInputSplit(
				"6").setPartitionMaxDistance(
				"1000").setOutputReducerCount(
				"4").setMapReduceHdfsHostPort(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).setMapReduceJobtrackerHostPort(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker()).setMapReduceHdfsBaseDir(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory()).addAllTypeNames(
				types).build();
		analyticMapreduceBlockingStub.dBScanCommand(request);
		return true;
	}

	public boolean nearestNeighborCommand() {
		final ArrayList<String> types = new ArrayList<>();
		types.add(GeoWaveGrpcTestUtils.typeName);
		final NearestNeighborCommandParameters request = NearestNeighborCommandParameters
				.newBuilder()
				.addParameters(
						GeoWaveGrpcTestUtils.storeName)
				.addAllTypeNames(
						types)
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.outputStoreName);
		final KdeCommandParameters request = KdeCommandParameters.newBuilder().addAllParameters(
				params).setCoverageName(
				"grpc_kde").setFeatureType(
				GeoWaveGrpcTestUtils.typeName).setHdfsHostPort(
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final RecalculateStatsCommandParameters request = RecalculateStatsCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setJsonFormatFlag(
						true)
				.build();
		coreStoreBlockingStub.recalculateStatsCommand(request);
		return true;
	}

	public String RemoveIndexCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.indexName);
		final RemoveIndexCommandParameters request = RemoveIndexCommandParameters.newBuilder().addAllParameters(
				params).build();
		final StringResponse resp = coreStoreBlockingStub.removeIndexCommand(request);
		return resp.getResponseValue();
	}

	public boolean VersionCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final VersionCommandParameters request = VersionCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.versionCommand(request);
		return true;
	}

	public String ListIndexCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final ListIndicesCommandParameters request = ListIndicesCommandParameters.newBuilder().addAllParameters(
				params).build();
		final StringResponse resp = coreStoreBlockingStub.listIndicesCommand(request);
		return resp.getResponseValue();
	}

	public String ListStatsCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final ListStatsCommandParameters request = ListStatsCommandParameters.newBuilder().addAllParameters(
				params).build();
		final StringResponse resp = coreStoreBlockingStub.listStatsCommand(request);
		return resp.getResponseValue();
	}

	public String AddIndexGroupCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.indexName + "-group");
		params.add(GeoWaveGrpcTestUtils.indexName);
		final AddIndexGroupCommandParameters request = AddIndexGroupCommandParameters.newBuilder().addAllParameters(
				params).build();
		final StringResponse resp = coreStoreBlockingStub.addIndexGroupCommand(request);
		return resp.getResponseValue();
	}

	public boolean ClearCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final ClearCommandParameters request = ClearCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.clearCommand(request);
		return true;
	}

	public String ListAdapterCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final ListTypesCommandParameters request = ListTypesCommandParameters.newBuilder().addAllParameters(
				params).build();
		final StringResponse resp = coreStoreBlockingStub.listTypesCommand(request);
		return resp.getResponseValue();
	}

	public String RemoveStoreCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final RemoveStoreCommandParameters request = RemoveStoreCommandParameters.newBuilder().addAllParameters(
				params).build();
		final StringResponse resp = coreStoreBlockingStub.removeStoreCommand(request);
		return resp.getResponseValue();
	}

	public boolean RemoveAdapterCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.typeName);
		final RemoveTypeCommandParameters request = RemoveTypeCommandParameters.newBuilder().addAllParameters(
				params).build();
		coreStoreBlockingStub.removeTypeCommand(request);
		return true;
	}

	public boolean RemoveStatCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.typeName);
		params.add("BOUNDING_BOX");
		final RemoveStatCommandParameters request = RemoveStatCommandParameters.newBuilder().addAllParameters(
				params).setFieldName(
				"geometry").build();
		coreStoreBlockingStub.removeStatCommand(request);
		return true;
	}

	public boolean CalculateStatCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.typeName);
		params.add("BOUNDING_BOX");
		final CalculateStatCommandParameters request = CalculateStatCommandParameters.newBuilder().addAllParameters(
				params).setFieldName(
				"geometry").build();
		coreStoreBlockingStub.calculateStatCommand(request);
		return true;
	}

	public String RemoveIndexGroupCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.indexName + "-group");
		final RemoveIndexGroupCommandParameters request = RemoveIndexGroupCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		final StringResponse resp = coreStoreBlockingStub.removeIndexGroupCommand(request);
		return resp.getResponseValue();
	}

	// Cli GeoServer
	public String GeoServerAddLayerCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerAddLayerCommandParameters request = GeoServerAddLayerCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setAdapterId(
						"GeometryTest")
				.setAddOption(
						"VECTOR")
				.setStyle(
						"default")
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerAddLayerCommand(
				request).getResponseValue();
	}

	public String GeoServerGetDatastoreCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerGetDatastoreCommandParameters request = GeoServerGetDatastoreCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerGetFeatureLayerCommandParameters request = GeoServerGetFeatureLayerCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerGetFeatureLayerCommand(
				request).getResponseValue();
	}

	public String GeoServerListCoverageStoresCommand() {
		final GeoServerListCoverageStoresCommandParameters request = GeoServerListCoverageStoresCommandParameters
				.newBuilder()
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerListCoverageStoresCommand(
				request).getResponseValue();
	}

	public List<String> GeoServerGetStoreAdapterCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerGetStoreAdapterCommandParameters request = GeoServerGetStoreAdapterCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerGetStoreAdapterCommand(
				request).getResponseValueList();
	}

	public String GeoServerGetCoverageCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerGetCoverageCommandParameters request = GeoServerGetCoverageCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerRemoveFeatureLayerCommandParameters request = GeoServerRemoveFeatureLayerCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveFeatureLayerCommand(
				request).getResponseValue();
	}

	public String GeoServerAddCoverageCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerAddCoverageCommandParameters request = GeoServerAddCoverageCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerRemoveWorkspaceCommandParameters request = GeoServerRemoveWorkspaceCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveWorkspaceCommand(
				request).getResponseValue();
	}

	public List<String> GeoServerListWorkspacesCommand() {
		final GeoServerListWorkspacesCommandParameters request = GeoServerListWorkspacesCommandParameters
				.newBuilder()
				.build();
		return cliGeoserverBlockingStub.geoServerListWorkspacesCommand(
				request).getResponseValueList();
	}

	public String GeoServerGetCoverageStoreCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerGetCoverageStoreCommandParameters request = GeoServerGetCoverageStoreCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final ConfigGeoServerCommandParameters request = ConfigGeoServerCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setWorkspace(
						"default")
				.setUsername(
						"user")
				.setPass(
						"default")
				.build();
		return cliGeoserverBlockingStub.configGeoServerCommand(
				request).getResponseValue();
	}

	public String GeoServerListCoveragesCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerListCoveragesCommandParameters request = GeoServerListCoveragesCommandParameters
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
		final GeoServerListStylesCommandParameters request = GeoServerListStylesCommandParameters.newBuilder().build();
		return cliGeoserverBlockingStub.geoServerListStylesCommand(
				request).getResponseValue();
	}

	public String GeoServerAddCoverageStoreCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerAddCoverageStoreCommandParameters request = GeoServerAddCoverageStoreCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerAddFeatureLayerCommandParameters request = GeoServerAddFeatureLayerCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		final GeoServerAddDatastoreCommandParameters request = GeoServerAddDatastoreCommandParameters
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
		final GeoServerListDatastoresCommandParameters request = GeoServerListDatastoresCommandParameters
				.newBuilder()
				.setWorkspace(
						"default")
				.build();
		return cliGeoserverBlockingStub.geoServerListDatastoresCommand(
				request).getResponseValue();
	}

	public String GeoServerSetLayerStyleCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerSetLayerStyleCommandParameters request = GeoServerSetLayerStyleCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerRemoveCoverageStoreCommandParameters request = GeoServerRemoveCoverageStoreCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerRemoveDatastoreCommandParameters request = GeoServerRemoveDatastoreCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerAddStyleCommandParameters request = GeoServerAddStyleCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.setStylesld(
						"styles-id")
				.build();
		return cliGeoserverBlockingStub.geoServerAddStyleCommand(
				request).getResponseValue();
	}

	public String GeoServerAddWorkspaceCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerAddWorkspaceCommandParameters request = GeoServerAddWorkspaceCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerAddWorkspaceCommand(
				request).getResponseValue();
	}

	public String GeoServerGetStyleCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerGetStyleCommandParameters request = GeoServerGetStyleCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerGetStyleCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveStyleCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerRemoveStyleCommandParameters request = GeoServerRemoveStyleCommandParameters
				.newBuilder()
				.addAllParameters(
						params)
				.build();
		return cliGeoserverBlockingStub.geoServerRemoveStyleCommand(
				request).getResponseValue();
	}

	public String GeoServerRemoveCoverageCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add("grpc");
		final GeoServerRemoveCoverageCommandParameters request = GeoServerRemoveCoverageCommandParameters
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
		final GeoServerListFeatureLayersCommandParameters request = GeoServerListFeatureLayersCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());

		final ArrayList<String> extensions = new ArrayList<>();

		final LocalToHdfsCommandParameters request = LocalToHdfsCommandParameters.newBuilder().addAllParameters(
				params).addAllExtensions(
				extensions).setFormats(
				"gpx").build();
		coreIngestBlockingStub.localToHdfsCommand(request);
		return true;
	}

	public boolean LocalToGeowaveCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexName);

		final ArrayList<String> extensions = new ArrayList<>();

		final LocalToGeowaveCommandParameters request = LocalToGeowaveCommandParameters.newBuilder().addAllParameters(
				params).addAllExtensions(
				extensions).setFormats(
				"gpx").setThreads(
				1).build();
		coreIngestBlockingStub.localToGeowaveCommand(request);
		return true;
	}

	public boolean MapReduceToGeowaveCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexName);

		final ArrayList<String> extensions = new ArrayList<>();
		final MapReduceToGeowaveCommandParameters request = MapReduceToGeowaveCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();

		final File tempDataDir = new File(
				"./" + TestUtils.TEST_CASE_BASE);
		String hdfsPath = "";
		try {
			hdfsPath = tempDataDir.toURI().toURL().toString();
		}
		catch (final MalformedURLException e) {
			return false;
		}

		// uncomment this line and comment-out the following to test s3 vs hdfs
		// params.add("s3://geowave-test/data/gdelt");
		params.add(hdfsPath + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexName);

		final ArrayList<String> extensions = new ArrayList<>();

		final SparkToGeowaveCommandParameters request = SparkToGeowaveCommandParameters.newBuilder().addAllParameters(
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
		params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexName);

		final ArrayList<String> extensions = new ArrayList<>();

		final LocalToMapReduceToGeowaveCommandParameters request = LocalToMapReduceToGeowaveCommandParameters
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.indexName);

		final ArrayList<String> extensions = new ArrayList<>();

		final KafkaToGeowaveCommandParameters request = KafkaToGeowaveCommandParameters.newBuilder().addAllParameters(
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
		final ListPluginsCommandParameters request = ListPluginsCommandParameters.newBuilder().build();
		return coreIngestBlockingStub.listPluginsCommand(
				request).getResponseValue();
	}

	public boolean LocalToKafkaCommand() {
		final ArrayList<String> params = new ArrayList<>();
		params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");

		final ArrayList<String> extensions = new ArrayList<>();

		String localhost = "localhost";
		try {
			localhost = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		}
		catch (final UnknownHostException e) {
			LOGGER.warn(
					"unable to get canonical hostname for localhost",
					e);
		}

		final LocalToKafkaCommandParameters request = LocalToKafkaCommandParameters.newBuilder().addAllParameters(
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
		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.outputStoreName);
		final KmeansSparkCommandParameters request = KmeansSparkCommandParameters.newBuilder().addAllParameters(
				params).setAppName(
				"test-app") // Spark app name
				.setHost(
						"localhost")
				// spark host
				.setMaster(
						"local[*]")
				// spark master designation Id
				.setTypeName(
						GeoWaveGrpcTestUtils.typeName)
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
		final ArrayList<String> params = new ArrayList<>();
		params.add("select * from %" + GeoWaveGrpcTestUtils.storeName + "|" + GeoWaveGrpcTestUtils.typeName);
		final SparkSqlCommandParameters request = SparkSqlCommandParameters.newBuilder().addAllParameters(
				params).setOutputStoreName(
				GeoWaveGrpcTestUtils.outputStoreName).setMaster(
				"local[*]").setAppName(
				"sparkSqlTestApp").setHost(
				"localhost").setOutputTypeName(
				GeoWaveGrpcTestUtils.typeName).setShowResults(
				5).build();
		analyticSparkBlockingStub.sparkSqlCommand(request);
		return true;
	}

	public boolean SpatialJoinCommand() {

		final ArrayList<String> params = new ArrayList<>();
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.storeName);
		params.add(GeoWaveGrpcTestUtils.outputStoreName);
		final SpatialJoinCommandParameters request = SpatialJoinCommandParameters.newBuilder().addAllParameters(
				params).setAppName(
				"test-app2").setMaster(
				"local[*]").setHost(
				"localhost").setLeftAdapterTypeName(
				GeoWaveGrpcTestUtils.typeName).setRightAdapterTypeName(
				GeoWaveGrpcTestUtils.typeName).setOutLeftAdapterTypeName(
				GeoWaveGrpcTestUtils.typeName + "_l").setOutRightAdapterTypeName(
				GeoWaveGrpcTestUtils.typeName + "_r").setPredicate(
				"GeomIntersects").setRadius(
				0.1).setNegativeTest(
				false).build();
		analyticSparkBlockingStub.spatialJoinCommand(request);
		return true;
	}

}
