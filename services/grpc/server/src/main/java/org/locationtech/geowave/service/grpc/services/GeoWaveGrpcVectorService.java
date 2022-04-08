/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.grpc.services;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.util.factory.FactoryRegistryException;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import org.locationtech.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import org.locationtech.geowave.adapter.vector.plugin.GeoWavePluginException;
import org.locationtech.geowave.core.geotime.store.query.api.SpatialTemporalConstraintsBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CQLQueryParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.FeatureAttributeProtos;
import org.locationtech.geowave.service.grpc.protobuf.FeatureProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.SpatialQueryParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SpatialTemporalQueryParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.TemporalConstraintsProtos;
import org.locationtech.geowave.service.grpc.protobuf.VectorGrpc;
import org.locationtech.geowave.service.grpc.protobuf.VectorIngestParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.VectorQueryParametersProtos;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;
import com.beust.jcommander.ParameterException;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcVectorService extends VectorGrpc.VectorImplBase implements
    GeoWaveGrpcServiceSpi {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcVectorService.class.getName());

  @Override
  public BindableService getBindableService() {
    return this;
  }

  @Override
  public void vectorQuery(
      final VectorQueryParametersProtos request,
      final StreamObserver<FeatureProtos> responseObserver) {
    final String storeName = request.getStoreName();
    final StoreLoader storeLoader = new StoreLoader(storeName);
    // first check to make sure the data store exists
    if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
      throw new ParameterException("Cannot find store name: " + storeLoader.getStoreName());
    }

    GeoWaveGTDataStore gtStore = null;
    try {
      gtStore = new GeoWaveGTDataStore(new GeoWavePluginConfig(storeLoader.getDataStorePlugin()));
    } catch (final IOException | GeoWavePluginException e) {
      LOGGER.error("Exception encountered instantiating GeoWaveGTDataStore", e);
      responseObserver.onError(e);
    }

    Filter filter = null;
    try {
      filter = ECQL.toFilter(request.getQuery());
    } catch (final CQLException e) {
      LOGGER.error("Exception encountered creating filter from CQL", e);
      responseObserver.onError(e);
    }

    ContentFeatureCollection featureCollection = null;
    try {
      final String typeName = request.getTypeName();
      featureCollection = gtStore.getFeatureSource(typeName).getFeatures(filter);
    } catch (final IOException | NullPointerException e) {
      LOGGER.error("Exception encountered getting feature collection", e);
      responseObserver.onError(e);
    }

    try (final SimpleFeatureIterator iterator = featureCollection.features()) {

      while (iterator.hasNext()) {
        final SimpleFeature simpleFeature = iterator.next();
        final SimpleFeatureType type = simpleFeature.getType();
        final FeatureProtos.Builder b = FeatureProtos.newBuilder();
        final FeatureAttributeProtos.Builder attBuilder = FeatureAttributeProtos.newBuilder();

        for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
          setAttributeBuilderValue(simpleFeature.getAttribute(i), attBuilder);
          b.putAttributes(type.getAttributeDescriptors().get(i).getLocalName(), attBuilder.build());
          /*
           * b.putAttributes( type.getAttributeDescriptors().get( i).getLocalName(),
           * simpleFeature.getAttribute(i) == null ? "" : simpleFeature.getAttribute(
           * i).toString());
           */
        }
        final FeatureProtos f = b.build();
        responseObserver.onNext(f);
      }
      responseObserver.onCompleted();
    } catch (final NullPointerException e) {
      LOGGER.error("Exception encountered", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public StreamObserver<VectorIngestParametersProtos> vectorIngest(
      final StreamObserver<StringResponseProtos> responseObserver) {
    return new StreamObserver<VectorIngestParametersProtos>() {
      private boolean firstFeature = true;
      private String storeName = null;
      private DataStore dataStore = null;
      private String typeName = null;
      private String indexName = null;
      private Writer<SimpleFeature> writer = null;

      private DataTypeAdapter adapter = null;
      private Index pIndex = null;
      private SimpleFeatureTypeBuilder typeBuilder = null;
      private SimpleFeatureBuilder featureBuilder = null;

      private static final int batchSize = 100;
      private int batchCount = 0;
      private int totalCount = 0;

      @Override
      public void onNext(final VectorIngestParametersProtos f) {
        if (firstFeature) {
          firstFeature = false;

          // parse top level required parameters
          storeName = f.getBaseParams().getStoreName();
          final StoreLoader storeLoader = new StoreLoader(storeName);

          typeName = f.getBaseParams().getTypeName();

          indexName = f.getBaseParams().getIndexName();

          // In order to store data we need to determine the type of
          // the feature data
          // This only needs to happen once
          if (typeBuilder == null) {
            typeBuilder = new SimpleFeatureTypeBuilder();

            for (final Map.Entry<String, FeatureAttributeProtos> mapEntry : f.getFeatureMap().entrySet()) {
              switch (mapEntry.getValue().getValueCase()) {
                case VALSTRING: {
                  typeBuilder.add(mapEntry.getKey(), String.class);
                  break;
                }
                case VALINT32: {
                  typeBuilder.add(mapEntry.getKey(), Integer.class);
                  break;
                }
                case VALINT64: {
                  typeBuilder.add(mapEntry.getKey(), Long.class);
                  break;
                }
                case VALFLOAT: {
                  typeBuilder.add(mapEntry.getKey(), Float.class);
                  break;
                }
                case VALDOUBLE: {
                  typeBuilder.add(mapEntry.getKey(), Double.class);
                  break;
                }
                case VALDATE: {
                  typeBuilder.add(mapEntry.getKey(), Date.class);
                  break;
                }
                case VALGEOMETRY: {
                  typeBuilder.add(mapEntry.getKey(), Geometry.class);
                  break;
                }
                default:
                  break;
              };
            }
          }
          // This a factory class that builds simple feature objects
          // based
          // on the
          // type
          typeBuilder.setName(typeName);
          final SimpleFeatureType featureType = typeBuilder.buildFeatureType();
          featureBuilder = new SimpleFeatureBuilder(featureType);

          // get a handle to the relevant stores
          if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
            throw new ParameterException("Cannot find store name: " + storeLoader.getStoreName());
          }

          dataStore = storeLoader.createDataStore();
          final PersistentAdapterStore adapterStore = storeLoader.createAdapterStore();
          final InternalAdapterStore internalAdapterStore =
              storeLoader.createInternalAdapterStore();
          final Short internalAdapterId = internalAdapterStore.getAdapterId(typeName);
          if (internalAdapterId != null) {
            adapter = adapterStore.getAdapter(internalAdapterId);
          } else {
            adapter = null;
          }
          if (adapter == null) {
            adapter = new FeatureDataAdapter(featureType);
          }

          // Load the Indexes
          final List<Index> indices =
              DataStoreUtils.loadIndices(storeLoader.createIndexStore(), indexName);

          // assuming one index for now
          pIndex = indices.get(0); // (PrimaryIndex)
          // indexStore.getIndex(indexId);
          if (pIndex == null) {
            throw new ParameterException("Failed to instantiate primary index");
          }

          // create index writer to actually write data
          dataStore.addType(adapter, pIndex);
          writer = dataStore.createWriter(adapter.getTypeName());
        } // end first-time initialization

        // Set the values for all the attributes in the feature
        for (final Map.Entry<String, FeatureAttributeProtos> attribute : f.getFeatureMap().entrySet()) {
          switch (attribute.getValue().getValueCase()) {
            case VALSTRING: {
              featureBuilder.set(attribute.getKey(), attribute.getValue().getValString());
              break;
            }
            case VALINT32: {
              featureBuilder.set(attribute.getKey(), attribute.getValue().getValInt32());
              break;
            }
            case VALINT64: {
              featureBuilder.set(attribute.getKey(), attribute.getValue().getValInt64());
              break;
            }
            case VALFLOAT: {
              featureBuilder.set(attribute.getKey(), attribute.getValue().getValFloat());
              break;
            }
            case VALDOUBLE: {
              featureBuilder.set(attribute.getKey(), attribute.getValue().getValDouble());
              break;
            }
            case VALDATE: {
              featureBuilder.set(
                  attribute.getKey(),
                  new Date(Timestamps.toMillis(attribute.getValue().getValDate())));
              break;
            }

            case VALGEOMETRY: {
              Geometry geom = null;
              try {
                geom =
                    new WKBReader(JTSFactoryFinder.getGeometryFactory()).read(
                        attribute.getValue().getValGeometry().toByteArray());
              } catch (FactoryRegistryException | org.locationtech.jts.io.ParseException e) {
                LOGGER.error("Failed to parse string for geometry", e);
              }

              if (geom != null) {
                featureBuilder.set(attribute.getKey(), geom);
              }
              break;
            }
            default:
              break;
          };
        }
        final SimpleFeature sf = featureBuilder.buildFeature(String.valueOf(totalCount));
        writer.write(sf);

        // The writer is finally flushed and closed in the methods for
        // onComplete and onError
        if (++batchCount >= batchSize) {
          // writer.flush();
          batchCount = 0;
        }

        final StringResponseProtos resp =
            StringResponseProtos.newBuilder().setResponseValue(
                String.valueOf(++totalCount)).build();
        responseObserver.onNext(resp);
      }

      @Override
      public void onError(final Throwable t) {
        LOGGER.error("Exception encountered during vectorIngest", t);
        writer.flush();
        writer.close();

        final StringResponseProtos resp =
            StringResponseProtos.newBuilder().setResponseValue("Error during ingest: ").build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        writer.flush();
        writer.close();
        final StringResponseProtos resp =
            StringResponseProtos.newBuilder().setResponseValue(
                "Ingest completed successfully").build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void cqlQuery(
      final CQLQueryParametersProtos request,
      final StreamObserver<FeatureProtos> responseObserver) {

    final String cql = request.getCql();
    final String storeName = request.getBaseParams().getStoreName();
    final StoreLoader storeLoader = new StoreLoader(storeName);

    String typeName = request.getBaseParams().getTypeName();
    String indexName = request.getBaseParams().getIndexName();

    if (typeName.equalsIgnoreCase("")) {
      typeName = null;
    }
    if (indexName.equalsIgnoreCase("")) {
      indexName = null;
    }

    // first check to make sure the data store exists
    if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
      throw new ParameterException("Cannot find store name: " + storeLoader.getStoreName());
    }

    // get a handle to the relevant stores
    final DataStore dataStore = storeLoader.createDataStore();

    VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    if (typeName != null) {
      bldr = bldr.addTypeName(typeName);
    }

    if (indexName != null) {
      bldr = bldr.indexName(indexName);
    }
    try (final CloseableIterator<SimpleFeature> iterator =
        dataStore.query(bldr.constraints(bldr.constraintsFactory().cqlConstraints(cql)).build())) {

      while (iterator.hasNext()) {
        final SimpleFeature simpleFeature = iterator.next();
        final SimpleFeatureType type = simpleFeature.getType();
        final FeatureProtos.Builder b = FeatureProtos.newBuilder();
        final FeatureAttributeProtos.Builder attBuilder = FeatureAttributeProtos.newBuilder();

        for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
          setAttributeBuilderValue(simpleFeature.getAttribute(i), attBuilder);
          b.putAttributes(type.getAttributeDescriptors().get(i).getLocalName(), attBuilder.build());
        }
        final FeatureProtos f = b.build();
        responseObserver.onNext(f);
      }
      responseObserver.onCompleted();
    }
  }

  @Override
  public void spatialQuery(
      final SpatialQueryParametersProtos request,
      final StreamObserver<FeatureProtos> responseObserver) {

    final String storeName = request.getBaseParams().getStoreName();
    final StoreLoader storeLoader = new StoreLoader(storeName);

    String typeName = request.getBaseParams().getTypeName();
    String indexName = request.getBaseParams().getIndexName();
    VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    if (typeName.equalsIgnoreCase("")) {
      typeName = null;
    } else {
      bldr = bldr.addTypeName(typeName);
    }
    if (indexName.equalsIgnoreCase("")) {
      indexName = null;
    } else {
      bldr = bldr.indexName(indexName);
    }

    // first check to make sure the data store exists
    if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
      throw new ParameterException("Cannot find store name: " + storeLoader.getStoreName());
    }

    final DataStore dataStore = storeLoader.createDataStore();

    Geometry queryGeom = null;

    try {
      queryGeom =
          new WKBReader(JTSFactoryFinder.getGeometryFactory()).read(
              request.getGeometry().toByteArray());
    } catch (final FactoryRegistryException | org.locationtech.jts.io.ParseException e) {
      LOGGER.error("Exception encountered creating query geometry", e);
    }

    try (final CloseableIterator<SimpleFeature> iterator =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                    queryGeom).build()).build())) {
      while (iterator.hasNext()) {
        final SimpleFeature simpleFeature = iterator.next();
        final SimpleFeatureType type = simpleFeature.getType();
        final FeatureProtos.Builder b = FeatureProtos.newBuilder();
        final FeatureAttributeProtos.Builder attBuilder = FeatureAttributeProtos.newBuilder();

        for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
          setAttributeBuilderValue(simpleFeature.getAttribute(i), attBuilder);
          b.putAttributes(type.getAttributeDescriptors().get(i).getLocalName(), attBuilder.build());
        }
        final FeatureProtos f = b.build();
        responseObserver.onNext(f);
      }
      responseObserver.onCompleted();
    }
  }

  @Override
  public void spatialTemporalQuery(
      final SpatialTemporalQueryParametersProtos request,
      final StreamObserver<FeatureProtos> responseObserver) {

    final String storeName = request.getSpatialParams().getBaseParams().getStoreName();
    final StoreLoader storeLoader = new StoreLoader(storeName);

    // first check to make sure the data store exists
    if (!storeLoader.loadFromConfig(GeoWaveGrpcServiceOptions.geowaveConfigFile)) {
      throw new ParameterException("Cannot find store name: " + storeLoader.getStoreName());
    }

    final DataStore dataStore = storeLoader.createDataStore();
    VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();

    String typeName = request.getSpatialParams().getBaseParams().getTypeName();
    String indexName = request.getSpatialParams().getBaseParams().getIndexName();

    if (typeName.equalsIgnoreCase("")) {
      typeName = null;
    } else {
      bldr = bldr.addTypeName(typeName);
    }
    if (indexName.equalsIgnoreCase("")) {
      indexName = null;
    } else {
      bldr = bldr.indexName(indexName);
    }

    final int constraintCount = request.getTemporalConstraintsCount();
    SpatialTemporalConstraintsBuilder stBldr =
        bldr.constraintsFactory().spatialTemporalConstraints();
    for (int i = 0; i < constraintCount; i++) {
      final TemporalConstraintsProtos t = request.getTemporalConstraints(i);
      stBldr.addTimeRange(
          Interval.of(
              Instant.ofEpochMilli(Timestamps.toMillis(t.getStartTime())),
              Instant.ofEpochMilli(Timestamps.toMillis(t.getEndTime()))));
    }

    Geometry queryGeom = null;

    try {
      queryGeom =
          new WKBReader(JTSFactoryFinder.getGeometryFactory()).read(
              request.getSpatialParams().getGeometry().toByteArray());
      stBldr = stBldr.spatialConstraints(queryGeom);

      stBldr =
          stBldr.spatialConstraintsCompareOperation(
              CompareOperation.valueOf(request.getCompareOperation()));
    } catch (final FactoryRegistryException | org.locationtech.jts.io.ParseException e) {
      LOGGER.error("Exception encountered creating query geometry", e);
    }

    try (final CloseableIterator<SimpleFeature> iterator =
        dataStore.query(bldr.constraints(stBldr.build()).build())) {
      while (iterator.hasNext()) {
        final SimpleFeature simpleFeature = iterator.next();
        final SimpleFeatureType type = simpleFeature.getType();
        final FeatureProtos.Builder b = FeatureProtos.newBuilder();
        final FeatureAttributeProtos.Builder attBuilder = FeatureAttributeProtos.newBuilder();

        for (int i = 0; i < type.getAttributeDescriptors().size(); i++) {
          setAttributeBuilderValue(simpleFeature.getAttribute(i), attBuilder);
          b.putAttributes(type.getAttributeDescriptors().get(i).getLocalName(), attBuilder.build());
        }
        final FeatureProtos f = b.build();
        responseObserver.onNext(f);
      }
      responseObserver.onCompleted();
    }
  }

  private void setAttributeBuilderValue(
      final Object simpleFeatureAttribute,
      final FeatureAttributeProtos.Builder attBuilder) {
    if (simpleFeatureAttribute != null) {
      switch (simpleFeatureAttribute.getClass().getSimpleName()) {
        case "String":
          attBuilder.setValString((String) simpleFeatureAttribute);
          break;

        case "Integer":
          attBuilder.setValInt32((Integer) simpleFeatureAttribute);
          break;

        case "Long":
          attBuilder.setValInt64((Long) simpleFeatureAttribute);
          break;

        case "Float":
          attBuilder.setValFloat((Float) simpleFeatureAttribute);
          break;

        case "Double":
          attBuilder.setValDouble((Double) simpleFeatureAttribute);
          break;

        case "Date":
          attBuilder.setValDate(Timestamps.fromMillis(((Date) simpleFeatureAttribute).getTime()));
          break;
        case "Geometry":
        case "Point":
        case "LineString":
        case "Polygon":
        case "GeometryCollection":
          attBuilder.setValGeometry(
              ByteString.copyFrom((new WKBWriter().write((Geometry) simpleFeatureAttribute))));
          break;
        default:
          break;
      };
    }
  }
}
