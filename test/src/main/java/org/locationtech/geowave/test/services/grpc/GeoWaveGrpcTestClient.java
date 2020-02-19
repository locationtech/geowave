/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services.grpc;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc.AnalyticMapreduceBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticSparkGrpc;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticSparkGrpc.AnalyticSparkBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CQLQueryParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.CalculateStatCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ClearStoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CliGeoserverGrpc.CliGeoserverBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.ConfigGeoServerCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ConfigHDFSCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.CoreCliGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreCliGrpc.CoreCliBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc.CoreIngestBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreMapreduceGrpc.CoreMapreduceBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.CoreStoreGrpc;
import org.locationtech.geowave.service.grpc.protobuf.CoreStoreGrpc.CoreStoreBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.DBScanCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.DescribeTypeCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.FeatureAttributeProtos;
import org.locationtech.geowave.service.grpc.protobuf.FeatureProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddCoverageStoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddDatastoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddFeatureLayerCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddLayerCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddStyleCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerAddWorkspaceCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetCoverageStoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetDatastoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetFeatureLayerCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStoreAdapterCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerGetStyleCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoverageStoresCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListCoveragesCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListDatastoresCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListFeatureLayersCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListStylesCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerListWorkspacesCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveCoverageStoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveDatastoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveFeatureLayerCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveStyleCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerRemoveWorkspaceCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoServerSetLayerStyleCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.MapStringStringResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.KafkaToGeowaveCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.KdeCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.KmeansSparkCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ListCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ListIndexPluginsCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ListIndicesCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ListIngestPluginsCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ListStatsCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ListStorePluginsCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.ListTypesCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.LocalToGeowaveCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.LocalToHdfsCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.LocalToKafkaCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.LocalToMapReduceToGeowaveCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.MapReduceToGeowaveCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.NearestNeighborCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.RecalculateStatsCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.RemoveIndexCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.RemoveStatCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.RemoveStoreCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.RemoveTypeCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SetCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SparkSqlCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SparkToGeowaveCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SpatialJoinCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SpatialQueryParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SpatialTemporalQueryParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.TemporalConstraintsProtos;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc.VectorBlockingStub;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc.VectorStub;
import org.locationtech.geowave.service.grpc.protobuf.VectorIngestParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.VectorQueryParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.VectorStoreParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.VersionCommandParametersProtos;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.ManagedChannel;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcTestClient {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcTestClient.class.getName());

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

  public GeoWaveGrpcTestClient(final String host, final int port) {
    this(
        NettyChannelBuilder.forAddress(host, port).nameResolverFactory(
            new DnsNameResolverProvider()).usePlaintext(true));
  }

  public GeoWaveGrpcTestClient(final NettyChannelBuilder channelBuilder) {
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

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  // Core CLI methods
  public void setCommand(final String key, final String val) {
    final ArrayList<String> params = new ArrayList<>();
    params.add(key);
    params.add(val);
    final SetCommandParametersProtos request =
        SetCommandParametersProtos.newBuilder().addAllParameters(params).build();
    coreCliBlockingStub.setCommand(request);
  }

  public Map<String, String> listCommand() {
    final ListCommandParametersProtos request = ListCommandParametersProtos.newBuilder().build();
    final MapStringStringResponseProtos response = coreCliBlockingStub.listCommand(request);
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
      final int lonStepDegs)
      throws InterruptedException, UnsupportedEncodingException, ParseException {
    LOGGER.info("Performing Vector Ingest...");
    final VectorStoreParametersProtos baseParams =
        VectorStoreParametersProtos.newBuilder().setStoreName(
            GeoWaveGrpcTestUtils.storeName).setTypeName(GeoWaveGrpcTestUtils.typeName).setIndexName(
                GeoWaveGrpcTestUtils.indexName).build();

    final CountDownLatch finishLatch = new CountDownLatch(1);
    final StreamObserver<StringResponseProtos> responseObserver =
        new StreamObserver<StringResponseProtos>() {

          @Override
          public void onNext(final StringResponseProtos value) {
            try {
              numFeaturesProcessed = Integer.parseInt(value.getResponseValue());
            } catch (final NumberFormatException e) {

            }
            LOGGER.info(value.getResponseValue());
          }

          @Override
          public void onError(final Throwable t) {
            LOGGER.error("Error: Vector Ingest failed.", t);
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            LOGGER.info("Finished Vector Ingest...");
            finishLatch.countDown();
          }
        };
    final StreamObserver<VectorIngestParametersProtos> requestObserver =
        vectorAsyncStub.vectorIngest(responseObserver);

    // Build up and add features to the request here...
    final VectorIngestParametersProtos.Builder requestBuilder =
        VectorIngestParametersProtos.newBuilder();
    final FeatureAttributeProtos.Builder attBuilder = FeatureAttributeProtos.newBuilder();
    for (int longitude = minLon; longitude <= maxLon; longitude += lonStepDegs) {
      for (int latitude = minLat; latitude <= maxLat; latitude += latStepDegs) {
        attBuilder.setValGeometry(
            copyFrom(
                new WKBWriter().write(
                    GeometryUtils.GEOMETRY_FACTORY.createPoint(
                        new Coordinate(longitude, latitude)))));
        requestBuilder.putFeature("geometry", attBuilder.build());

        final TimeZone tz = TimeZone.getTimeZone("UTC");
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to
        // indicate UTC,
        // no timezone offset
        df.setTimeZone(tz);
        attBuilder.setValDate(
            Timestamps.fromMillis(
                (df.parse(GeoWaveGrpcTestUtils.temporalQueryStartTime).getTime()
                    + df.parse(GeoWaveGrpcTestUtils.temporalQueryEndTime).getTime()) / 2));
        requestBuilder.putFeature("TimeStamp", attBuilder.build());

        attBuilder.setValDouble(latitude);
        requestBuilder.putFeature("Latitude", attBuilder.build());

        attBuilder.setValDouble(longitude);
        requestBuilder.putFeature("Longitude", attBuilder.build());

        final VectorIngestParametersProtos params =
            requestBuilder.setBaseParams(baseParams).build();
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
    if (!finishLatch.await(15, TimeUnit.MINUTES)) {
      LOGGER.warn("Vector Ingest can not finish within 5 minutes");
    }
  }

  public ArrayList<FeatureProtos> vectorQuery() throws UnsupportedEncodingException {
    LOGGER.info("Performing Vector Query...");
    final VectorQueryParametersProtos request =
        VectorQueryParametersProtos.newBuilder().setStoreName(
            GeoWaveGrpcTestUtils.storeName).setTypeName(GeoWaveGrpcTestUtils.typeName).setQuery(
                GeoWaveGrpcTestUtils.cqlSpatialQuery).build();

    final Iterator<FeatureProtos> features = vectorBlockingStub.vectorQuery(request);
    final ArrayList<FeatureProtos> feature_list = new ArrayList<>();

    // iterate over features
    for (int i = 1; features.hasNext(); i++) {
      final FeatureProtos feature = features.next();
      feature_list.add(feature);
    }
    return feature_list;
  }

  private static ByteString copyFrom(final byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  public ArrayList<FeatureProtos> cqlQuery() throws UnsupportedEncodingException {
    LOGGER.info("Performing CQL Query...");
    final VectorStoreParametersProtos baseParams =
        VectorStoreParametersProtos.newBuilder().setStoreName(
            GeoWaveGrpcTestUtils.storeName).setTypeName(GeoWaveGrpcTestUtils.typeName).setIndexName(
                GeoWaveGrpcTestUtils.indexName).build();

    final CQLQueryParametersProtos request =
        CQLQueryParametersProtos.newBuilder().setBaseParams(baseParams).setCql(
            GeoWaveGrpcTestUtils.cqlSpatialQuery).build();

    Iterator<FeatureProtos> features;
    final ArrayList<FeatureProtos> feature_list = new ArrayList<>();
    features = vectorBlockingStub.cqlQuery(request);

    // iterate over features
    for (int i = 1; features.hasNext(); i++) {
      final FeatureProtos feature = features.next();
      feature_list.add(feature);
    }
    return feature_list;
  }

  public ArrayList<FeatureProtos> spatialQuery() throws UnsupportedEncodingException {
    LOGGER.info("Performing Spatial Query...");
    final VectorStoreParametersProtos baseParams =
        VectorStoreParametersProtos.newBuilder().setStoreName(
            GeoWaveGrpcTestUtils.storeName).setTypeName(GeoWaveGrpcTestUtils.typeName).setIndexName(
                GeoWaveGrpcTestUtils.indexName).build();

    final SpatialQueryParametersProtos request =
        SpatialQueryParametersProtos.newBuilder().setBaseParams(baseParams).setGeometry(
            copyFrom(GeoWaveGrpcTestUtils.wkbSpatialQuery)).build();

    Iterator<FeatureProtos> features;
    final ArrayList<FeatureProtos> feature_list = new ArrayList<>();
    features = vectorBlockingStub.spatialQuery(request);

    // iterate over features
    for (int i = 1; features.hasNext(); i++) {
      final FeatureProtos feature = features.next();
      feature_list.add(feature);
    }
    return feature_list;
  }

  public ArrayList<FeatureProtos> spatialTemporalQuery() throws ParseException {
    LOGGER.info("Performing Spatial Temporal Query...");
    final VectorStoreParametersProtos baseParams =
        VectorStoreParametersProtos.newBuilder().setStoreName(
            GeoWaveGrpcTestUtils.storeName).build();

    final TimeZone tz = TimeZone.getTimeZone("UTC");
    final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate
    // UTC,
    // no timezone offset
    df.setTimeZone(tz);

    final SpatialQueryParametersProtos spatialQuery =
        SpatialQueryParametersProtos.newBuilder().setBaseParams(baseParams).setGeometry(
            copyFrom(GeoWaveGrpcTestUtils.wkbSpatialQuery)).build();
    final TemporalConstraintsProtos t =
        TemporalConstraintsProtos.newBuilder().setStartTime(
            Timestamps.fromMillis(
                df.parse(GeoWaveGrpcTestUtils.temporalQueryStartTime).getTime())).setEndTime(
                    Timestamps.fromMillis(
                        df.parse(GeoWaveGrpcTestUtils.temporalQueryEndTime).getTime())).build();
    final SpatialTemporalQueryParametersProtos request =
        SpatialTemporalQueryParametersProtos.newBuilder().setSpatialParams(
            spatialQuery).addTemporalConstraints(0, t).setCompareOperation("CONTAINS").build();

    Iterator<FeatureProtos> features;
    final ArrayList<FeatureProtos> feature_list = new ArrayList<>();
    features = vectorBlockingStub.spatialTemporalQuery(request);

    // iterate over features
    while (features.hasNext()) {
      final FeatureProtos feature = features.next();
      feature_list.add(feature);
    }
    return feature_list;
  }

  // Core Mapreduce
  public boolean configHDFSCommand() {
    final ConfigHDFSCommandParametersProtos request =
        ConfigHDFSCommandParametersProtos.newBuilder().addParameters(
            GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).build();
    coreMapreduceBlockingStub.configHDFSCommand(request);
    return true;
  }

  // Analytic Mapreduce
  public boolean dbScanCommand() {
    final ArrayList<String> types = new ArrayList<>();
    types.add(GeoWaveGrpcTestUtils.typeName);
    final DBScanCommandParametersProtos request =
        DBScanCommandParametersProtos.newBuilder().addParameters(
            GeoWaveGrpcTestUtils.storeName).setClusteringMaxIterations(
                "5").setClusteringMinimumSize("10").setExtractMinInputSplit(
                    "2").setExtractMaxInputSplit("6").setPartitionMaxDistance(
                        "1000").setOutputReducerCount("4").setMapReduceHdfsHostPort(
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
    final NearestNeighborCommandParametersProtos request =
        NearestNeighborCommandParametersProtos.newBuilder().addParameters(
            GeoWaveGrpcTestUtils.storeName).addAllTypeNames(types).setExtractQuery(
                GeoWaveGrpcTestUtils.wktSpatialQuery).setExtractMinInputSplit(
                    "2").setExtractMaxInputSplit("6").setPartitionMaxDistance(
                        "10").setOutputReducerCount("4").setMapReduceHdfsHostPort(
                            GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).setMapReduceJobtrackerHostPort(
                                GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker()).setOutputHdfsOutputPath(
                                    GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory()
                                        + "/GrpcNearestNeighbor").setMapReduceHdfsBaseDir(
                                            GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory()).build();
    analyticMapreduceBlockingStub.nearestNeighborCommand(request);
    return true;
  }

  public boolean kdeCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.outputStoreName);
    final KdeCommandParametersProtos request =
        KdeCommandParametersProtos.newBuilder().addAllParameters(params).setCoverageName(
            "grpc_kde").setFeatureType(GeoWaveGrpcTestUtils.typeName).setHdfsHostPort(
                GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs()).setJobTrackerOrResourceManHostPort(
                    GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker()).setMinLevel(
                        5).setMaxLevel(26).setMinSplits(32).setMaxSplits(32).setTileSize(1).build();
    analyticMapreduceBlockingStub.kdeCommand(request);
    return true;
  }

  // Core Store
  public boolean RecalculateStatsCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final RecalculateStatsCommandParametersProtos request =
        RecalculateStatsCommandParametersProtos.newBuilder().addAllParameters(
            params).setJsonFormatFlag(true).build();
    coreStoreBlockingStub.recalculateStatsCommand(request);
    return true;
  }

  public String RemoveIndexCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.indexName);
    final RemoveIndexCommandParametersProtos request =
        RemoveIndexCommandParametersProtos.newBuilder().addAllParameters(params).build();
    final StringResponseProtos resp = coreStoreBlockingStub.removeIndexCommand(request);
    return resp.getResponseValue();
  }

  public boolean VersionCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final VersionCommandParametersProtos request =
        VersionCommandParametersProtos.newBuilder().addAllParameters(params).build();
    coreStoreBlockingStub.versionCommand(request);
    return true;
  }

  public String ListIndexCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final ListIndicesCommandParametersProtos request =
        ListIndicesCommandParametersProtos.newBuilder().addAllParameters(params).build();
    final StringResponseProtos resp = coreStoreBlockingStub.listIndicesCommand(request);
    return resp.getResponseValue();
  }

  public String ListStatsCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final ListStatsCommandParametersProtos request =
        ListStatsCommandParametersProtos.newBuilder().addAllParameters(params).build();
    final StringResponseProtos resp = coreStoreBlockingStub.listStatsCommand(request);
    return resp.getResponseValue();
  }

  public boolean ClearCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final ClearStoreCommandParametersProtos request =
        ClearStoreCommandParametersProtos.newBuilder().addAllParameters(params).build();
    coreStoreBlockingStub.clearStoreCommand(request);
    return true;
  }

  public String ListAdapterCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final ListTypesCommandParametersProtos request =
        ListTypesCommandParametersProtos.newBuilder().addAllParameters(params).build();
    final StringResponseProtos resp = coreStoreBlockingStub.listTypesCommand(request);
    return resp.getResponseValue();
  }

  public boolean DescribeAdapterCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.typeName);
    final DescribeTypeCommandParametersProtos request =
        DescribeTypeCommandParametersProtos.newBuilder().addAllParameters(params).build();
    coreStoreBlockingStub.describeTypeCommand(request);
    return true;
  }

  public String RemoveStoreCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final RemoveStoreCommandParametersProtos request =
        RemoveStoreCommandParametersProtos.newBuilder().addAllParameters(params).build();
    final StringResponseProtos resp = coreStoreBlockingStub.removeStoreCommand(request);
    return resp.getResponseValue();
  }

  public boolean RemoveAdapterCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.typeName);
    final RemoveTypeCommandParametersProtos request =
        RemoveTypeCommandParametersProtos.newBuilder().addAllParameters(params).build();
    coreStoreBlockingStub.removeTypeCommand(request);
    return true;
  }

  public boolean RemoveStatCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.typeName);
    params.add("BOUNDING_BOX");
    final RemoveStatCommandParametersProtos request =
        RemoveStatCommandParametersProtos.newBuilder().addAllParameters(params).setFieldName(
            "geometry").build();
    coreStoreBlockingStub.removeStatCommand(request);
    return true;
  }

  public boolean CalculateStatCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.typeName);
    params.add("BOUNDING_BOX");
    final CalculateStatCommandParametersProtos request =
        CalculateStatCommandParametersProtos.newBuilder().addAllParameters(params).setFieldName(
            "geometry").build();
    coreStoreBlockingStub.calculateStatCommand(request);
    return true;
  }

  // Cli GeoServer
  public String GeoServerAddLayerCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerAddLayerCommandParametersProtos request =
        GeoServerAddLayerCommandParametersProtos.newBuilder().addAllParameters(params).setAdapterId(
            "GeometryTest").setAddOption("VECTOR").setStyle("default").setWorkspace(
                "default").build();
    return cliGeoserverBlockingStub.geoServerAddLayerCommand(request).getResponseValue();
  }

  public String GeoServerGetDatastoreCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerGetDatastoreCommandParametersProtos request =
        GeoServerGetDatastoreCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").build();
    return cliGeoserverBlockingStub.geoServerGetDatastoreCommand(request).getResponseValue();
  }

  public String GeoServerGetFeatureProtosLayerCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerGetFeatureLayerCommandParametersProtos request =
        GeoServerGetFeatureLayerCommandParametersProtos.newBuilder().addAllParameters(
            params).build();
    return cliGeoserverBlockingStub.geoServerGetFeatureLayerCommand(request).getResponseValue();
  }

  public String GeoServerListCoverageStoresCommand() {
    final GeoServerListCoverageStoresCommandParametersProtos request =
        GeoServerListCoverageStoresCommandParametersProtos.newBuilder().setWorkspace(
            "default").build();
    return cliGeoserverBlockingStub.geoServerListCoverageStoresCommand(request).getResponseValue();
  }

  public List<String> GeoServerGetStoreAdapterCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerGetStoreAdapterCommandParametersProtos request =
        GeoServerGetStoreAdapterCommandParametersProtos.newBuilder().addAllParameters(
            params).build();
    return cliGeoserverBlockingStub.geoServerGetStoreAdapterCommand(request).getResponseValueList();
  }

  public String GeoServerGetCoverageCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerGetCoverageCommandParametersProtos request =
        GeoServerGetCoverageCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").setCvgstore("test_cvg_store").build();
    return cliGeoserverBlockingStub.geoServerGetCoverageCommand(request).getResponseValue();
  }

  public String GeoServerRemoveFeatureProtosLayerCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerRemoveFeatureLayerCommandParametersProtos request =
        GeoServerRemoveFeatureLayerCommandParametersProtos.newBuilder().addAllParameters(
            params).build();
    return cliGeoserverBlockingStub.geoServerRemoveFeatureLayerCommand(request).getResponseValue();
  }

  public String GeoServerAddCoverageCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerAddCoverageCommandParametersProtos request =
        GeoServerAddCoverageCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").setCvgstore("test_cvg_store").build();
    return cliGeoserverBlockingStub.geoServerAddCoverageCommand(request).getResponseValue();
  }

  public String GeoServerRemoveWorkspaceCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerRemoveWorkspaceCommandParametersProtos request =
        GeoServerRemoveWorkspaceCommandParametersProtos.newBuilder().addAllParameters(
            params).build();
    return cliGeoserverBlockingStub.geoServerRemoveWorkspaceCommand(request).getResponseValue();
  }

  public List<String> GeoServerListWorkspacesCommand() {
    final GeoServerListWorkspacesCommandParametersProtos request =
        GeoServerListWorkspacesCommandParametersProtos.newBuilder().build();
    return cliGeoserverBlockingStub.geoServerListWorkspacesCommand(request).getResponseValueList();
  }

  public String GeoServerGetCoverageStoreCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerGetCoverageStoreCommandParametersProtos request =
        GeoServerGetCoverageStoreCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").build();
    return cliGeoserverBlockingStub.geoServerGetCoverageStoreCommand(request).getResponseValue();
  }

  public String ConfigGeoServerCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final ConfigGeoServerCommandParametersProtos request =
        ConfigGeoServerCommandParametersProtos.newBuilder().addAllParameters(params).setWorkspace(
            "default").setUsername("user").setPass("default").build();
    return cliGeoserverBlockingStub.configGeoServerCommand(request).getResponseValue();
  }

  public String GeoServerListCoveragesCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerListCoveragesCommandParametersProtos request =
        GeoServerListCoveragesCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").build();
    return cliGeoserverBlockingStub.geoServerListCoveragesCommand(request).getResponseValue();
  }

  public String GeoServerListStylesCommand() {
    final GeoServerListStylesCommandParametersProtos request =
        GeoServerListStylesCommandParametersProtos.newBuilder().build();
    return cliGeoserverBlockingStub.geoServerListStylesCommand(request).getResponseValue();
  }

  public String GeoServerAddCoverageStoreCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerAddCoverageStoreCommandParametersProtos request =
        GeoServerAddCoverageStoreCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").setCoverageStore(
                "coverage-store").setEqualizeHistogramOverride(false).setScaleTo8Bit(
                    false).setInterpolationOverride("0").build();
    return cliGeoserverBlockingStub.geoServerAddCoverageStoreCommand(request).getResponseValue();
  }

  public String GeoServerAddFeatureProtosLayerCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerAddFeatureLayerCommandParametersProtos request =
        GeoServerAddFeatureLayerCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").setDatastore("grpc").build();
    return cliGeoserverBlockingStub.geoServerAddFeatureLayerCommand(request).getResponseValue();
  }

  public String GeoServerAddDatastoreCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    final GeoServerAddDatastoreCommandParametersProtos request =
        GeoServerAddDatastoreCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").setDatastore("grpc-store").build();
    return cliGeoserverBlockingStub.geoServerAddDatastoreCommand(request).getResponseValue();
  }

  public String GeoServerListDatastoresCommand() {
    final GeoServerListDatastoresCommandParametersProtos request =
        GeoServerListDatastoresCommandParametersProtos.newBuilder().setWorkspace("default").build();
    return cliGeoserverBlockingStub.geoServerListDatastoresCommand(request).getResponseValue();
  }

  public String GeoServerSetLayerStyleCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerSetLayerStyleCommandParametersProtos request =
        GeoServerSetLayerStyleCommandParametersProtos.newBuilder().addAllParameters(
            params).setStyleName("test-style").build();
    return cliGeoserverBlockingStub.geoServerSetLayerStyleCommand(request).getResponseValue();
  }

  public String GeoServerRemoveCoverageStoreCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerRemoveCoverageStoreCommandParametersProtos request =
        GeoServerRemoveCoverageStoreCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").build();
    return cliGeoserverBlockingStub.geoServerRemoveCoverageStoreCommand(request).getResponseValue();
  }

  public String GeoServerRemoveDatastoreCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerRemoveDatastoreCommandParametersProtos request =
        GeoServerRemoveDatastoreCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").build();
    return cliGeoserverBlockingStub.geoServerRemoveDatastoreCommand(request).getResponseValue();
  }

  public String GeoServerAddStyleCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerAddStyleCommandParametersProtos request =
        GeoServerAddStyleCommandParametersProtos.newBuilder().addAllParameters(params).setStylesld(
            "styles-id").build();
    return cliGeoserverBlockingStub.geoServerAddStyleCommand(request).getResponseValue();
  }

  public String GeoServerAddWorkspaceCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerAddWorkspaceCommandParametersProtos request =
        GeoServerAddWorkspaceCommandParametersProtos.newBuilder().addAllParameters(params).build();
    return cliGeoserverBlockingStub.geoServerAddWorkspaceCommand(request).getResponseValue();
  }

  public String GeoServerGetStyleCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerGetStyleCommandParametersProtos request =
        GeoServerGetStyleCommandParametersProtos.newBuilder().addAllParameters(params).build();
    return cliGeoserverBlockingStub.geoServerGetStyleCommand(request).getResponseValue();
  }

  public String GeoServerRemoveStyleCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerRemoveStyleCommandParametersProtos request =
        GeoServerRemoveStyleCommandParametersProtos.newBuilder().addAllParameters(params).build();
    return cliGeoserverBlockingStub.geoServerRemoveStyleCommand(request).getResponseValue();
  }

  public String GeoServerRemoveCoverageCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add("grpc");
    final GeoServerRemoveCoverageCommandParametersProtos request =
        GeoServerRemoveCoverageCommandParametersProtos.newBuilder().addAllParameters(
            params).setWorkspace("default").setCvgstore("cvg-store").build();
    return cliGeoserverBlockingStub.geoServerRemoveCoverageCommand(request).getResponseValue();
  }

  public String GeoServerListFeatureProtosLayersCommand() {
    final GeoServerListFeatureLayersCommandParametersProtos request =
        GeoServerListFeatureLayersCommandParametersProtos.newBuilder().setWorkspace(
            "default").setDatastore("cvg-store").setGeowaveOnly(true).build();
    return cliGeoserverBlockingStub.geoServerListFeatureLayersCommand(request).getResponseValue();
  }

  // Core Ingest
  public boolean LocalToHdfsCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
    params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());

    final ArrayList<String> extensions = new ArrayList<>();

    final LocalToHdfsCommandParametersProtos request =
        LocalToHdfsCommandParametersProtos.newBuilder().addAllParameters(params).addAllExtensions(
            extensions).setFormats("gpx").build();
    coreIngestBlockingStub.localToHdfsCommand(request);
    return true;
  }

  public boolean LocalToGeoWaveCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.indexName);

    final ArrayList<String> extensions = new ArrayList<>();

    final LocalToGeowaveCommandParametersProtos request =
        LocalToGeowaveCommandParametersProtos.newBuilder().addAllParameters(
            params).addAllExtensions(extensions).setFormats("gpx").setThreads(1).build();
    coreIngestBlockingStub.localToGeowaveCommand(request);
    return true;
  }

  public boolean MapReduceToGeoWaveCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.indexName);

    final ArrayList<String> extensions = new ArrayList<>();
    final MapReduceToGeowaveCommandParametersProtos request =
        MapReduceToGeowaveCommandParametersProtos.newBuilder().addAllParameters(
            params).addAllExtensions(extensions).setFormats("gpx").setJobTrackerHostPort(
                GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker()).build();
    coreIngestBlockingStub.mapReduceToGeowaveCommand(request);
    return true;
  }

  public boolean SparkToGeoWaveCommand() {
    final ArrayList<String> params = new ArrayList<>();

    final File tempDataDir = new File("./" + TestUtils.TEST_CASE_BASE);
    String hdfsPath = "";
    try {
      hdfsPath = tempDataDir.toURI().toURL().toString();
    } catch (final MalformedURLException e) {
      return false;
    }

    // uncomment this line and comment-out the following to test s3 vs hdfs
    // params.add("s3://geowave-test/data/gdelt");
    params.add(hdfsPath + "osm_gpx_test_case/");
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.indexName);

    final ArrayList<String> extensions = new ArrayList<>();

    final SparkToGeowaveCommandParametersProtos request =
        SparkToGeowaveCommandParametersProtos.newBuilder().addAllParameters(
            params).addAllExtensions(extensions).setFormats("gpx").setAppName(
                "CoreGeoWaveSparkITs").setMaster("local[*]").setHost("localhost").setNumExecutors(
                    1).setNumCores(1).build();
    coreIngestBlockingStub.sparkToGeowaveCommand(request);
    return true;
  }

  public boolean LocalToMapReduceToGeoWaveCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");
    params.add(GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfsBaseDirectory());
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.indexName);

    final ArrayList<String> extensions = new ArrayList<>();

    final LocalToMapReduceToGeowaveCommandParametersProtos request =
        LocalToMapReduceToGeowaveCommandParametersProtos.newBuilder().addAllParameters(
            params).addAllExtensions(extensions).setFormats("gpx").setJobTrackerHostPort(
                GeoWaveGrpcTestUtils.getMapReduceTestEnv().getJobtracker()).build();
    coreIngestBlockingStub.localToMapReduceToGeowaveCommand(request);
    return true;
  }

  public boolean KafkaToGeoWaveCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.indexName);

    final ArrayList<String> extensions = new ArrayList<>();

    final KafkaToGeowaveCommandParametersProtos request =
        KafkaToGeowaveCommandParametersProtos.newBuilder().addAllParameters(
            params).addAllExtensions(extensions).setFormats("gpx").setGroupId(
                "testGroup").setZookeeperConnect(
                    GeoWaveGrpcTestUtils.getZookeeperTestEnv().getZookeeper()).setAutoOffsetReset(
                        "smallest").setFetchMessageMaxBytes("5000000").setConsumerTimeoutMs(
                            "5000").setReconnectOnTimeout(false).setBatchSize(10000).build();
    coreIngestBlockingStub.kafkaToGeowaveCommand(request);
    return true;
  }

  public String ListIngestPluginsCommand() {
    final ListIngestPluginsCommandParametersProtos request =
        ListIngestPluginsCommandParametersProtos.newBuilder().build();
    return coreIngestBlockingStub.listIngestPluginsCommand(request).getResponseValue();
  }

  public String ListIndexPluginsCommand() {
    final ListIndexPluginsCommandParametersProtos request =
        ListIndexPluginsCommandParametersProtos.newBuilder().build();
    return coreStoreBlockingStub.listIndexPluginsCommand(request).getResponseValue();
  }

  public String ListStorePluginsCommand() {
    final ListStorePluginsCommandParametersProtos request =
        ListStorePluginsCommandParametersProtos.newBuilder().build();
    return coreStoreBlockingStub.listStorePluginsCommand(request).getResponseValue();
  }

  public boolean LocalToKafkaCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/");

    final ArrayList<String> extensions = new ArrayList<>();

    String localhost = "localhost";
    try {
      localhost = java.net.InetAddress.getLocalHost().getCanonicalHostName();
    } catch (final UnknownHostException e) {
      LOGGER.warn("unable to get canonical hostname for localhost", e);
    }

    final LocalToKafkaCommandParametersProtos request =
        LocalToKafkaCommandParametersProtos.newBuilder().addAllParameters(params).addAllExtensions(
            extensions).setFormats("gpx").setMetadataBrokerList(
                localhost + ":9092").setRequestRequiredAcks("1").setProducerType(
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
    final KmeansSparkCommandParametersProtos request =
        KmeansSparkCommandParametersProtos.newBuilder().addAllParameters(params).setAppName(
            "test-app") // Spark
            // app
            // name
            .setHost("localhost")
            // spark host
            .setMaster("local[*]")
            // spark master designation Id
            .setTypeName(GeoWaveGrpcTestUtils.typeName).setNumClusters(2)
            //
            .setNumIterations(2).setEpsilon(20.0).setUseTime(false).setGenerateHulls(true)
            // optional
            .setComputeHullData(true)
            // optional
            .setCqlFilter(GeoWaveGrpcTestUtils.cqlSpatialQuery).setMinSplits(1).setMaxSplits(
                4).setCentroidTypeName("poly").setHullTypeName("poly-hull").build();
    analyticSparkBlockingStub.kmeansSparkCommand(request);
    return true;
  }

  public boolean SparkSqlCommand() {
    final ArrayList<String> params = new ArrayList<>();
    params.add(
        "select * from %" + GeoWaveGrpcTestUtils.storeName + "|" + GeoWaveGrpcTestUtils.typeName);
    final SparkSqlCommandParametersProtos request =
        SparkSqlCommandParametersProtos.newBuilder().addAllParameters(params).setOutputStoreName(
            GeoWaveGrpcTestUtils.outputStoreName).setMaster("local[*]").setAppName(
                "sparkSqlTestApp").setHost("localhost").setOutputTypeName(
                    GeoWaveGrpcTestUtils.typeName).setShowResults(5).build();
    analyticSparkBlockingStub.sparkSqlCommand(request);
    return true;
  }

  public boolean SpatialJoinCommand() {

    final ArrayList<String> params = new ArrayList<>();
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.storeName);
    params.add(GeoWaveGrpcTestUtils.outputStoreName);
    final SpatialJoinCommandParametersProtos request =
        SpatialJoinCommandParametersProtos.newBuilder().addAllParameters(params).setAppName(
            "test-app2").setMaster("local[*]").setHost("localhost").setLeftAdapterTypeName(
                GeoWaveGrpcTestUtils.typeName).setRightAdapterTypeName(
                    GeoWaveGrpcTestUtils.typeName).setOutLeftAdapterTypeName(
                        GeoWaveGrpcTestUtils.typeName + "_l").setOutRightAdapterTypeName(
                            GeoWaveGrpcTestUtils.typeName + "_r").setPredicate(
                                "GeomIntersects").setRadius(0.1).setNegativeTest(false).build();
    analyticSparkBlockingStub.spatialJoinCommand(request);
    return true;
  }
}
