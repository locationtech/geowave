/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.adapter;

import java.io.IOException;
import java.util.Date;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.util.factory.FactoryRegistryException;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveSpatialField;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveTemporalField;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.store.query.SpatialTemporalConstraintsBuilderImpl;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * This class provides an example of how to create a {@link DataTypeAdapter} for a custom data type.
 * This allows a user to directly write, index, and query their own data types from a GeoWave data
 * store without having to translate to and from a `SimpleFeature`. It differs from the
 * {@link CustomAdapterExample} in that it does not require a new adapter to be registered with the
 * persistable registry and is not suitable for some more complex data types. The basic data type
 * adapter uses reflection or annotations to infer the fields of a data type.
 */
public class BasicDataTypeAdapterExample {

  private DataStore dataStore;
  private DataTypeAdapter<POI> adapter;
  private DataTypeAdapter<AnnotatedPOI> annotatedAdapter;
  private Index spatialIndex;

  public static void main(final String[] args) throws IOException, CQLException {

    final BasicDataTypeAdapterExample example = new BasicDataTypeAdapterExample();
    example.run();
  }

  public void run() {
    // Create an in-memory data store to use with this example
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    // Create the adapter for our POI class with the type name `POI`
    adapter = BasicDataTypeAdapter.newAdapter("POI", POI.class, "name");

    // Create the adapter for our Annotated POI class with the type name `AnnotatedPOI`
    annotatedAdapter =
        BasicDataTypeAdapter.newAdapter("AnnotatedPOI", AnnotatedPOI.class, "alternateFieldName");

    // Create the spatial index
    spatialIndex = new SpatialIndexBuilder().createIndex();

    // Add the types to the data store with the spatial index
    dataStore.addType(adapter, spatialIndex);
    dataStore.addType(annotatedAdapter, spatialIndex);

    // Ingest the data into a spatial index
    ingestData();

    // Perform a spatial query on the data
    querySpatial();
  }

  private void ingestData() {
    try (Writer<POI> writer = dataStore.createWriter(adapter.getTypeName())) {
      // We can directly write `POI` instances to the data store
      writer.write(new POI("Eiffel Tower", 48.858093, 2.294694));
      writer.write(new POI("Roman Colosseum", 41.890167, 12.492269));
      writer.write(new POI("Great Pyramid of Giza", 29.979176, 31.134357));
      writer.write(new POI("Mount Everest", 27.986065, 86.922623));
    }

    try (Writer<AnnotatedPOI> writer = dataStore.createWriter(annotatedAdapter.getTypeName())) {
      // We can directly write `AnnotatedPOI` instances to the data store
      writer.write(new AnnotatedPOI("Eiffel Tower", new Date(), 48.858093, 2.294694));
      writer.write(new AnnotatedPOI("Roman Colosseum", new Date(), 41.890167, 12.492269));
      writer.write(new AnnotatedPOI("Great Pyramid of Giza", new Date(), 29.979176, 31.134357));
      writer.write(new AnnotatedPOI("Mount Everest", new Date(), 27.986065, 86.922623));
    }
  }

  private void querySpatial() {
    try {
      // This bounding box represents approximately Europe, so only the European POIs will be
      // queried
      final String queryPolygonDefinition =
          "POLYGON (( "
              + "-10.55 35.96, "
              + "-10.55 71.30, "
              + "56.16 71.30, "
              + "56.16 35.96, "
              + "-10.55 35.96"
              + "))";
      final Geometry queryPolygon =
          new WKTReader(JTSFactoryFinder.getGeometryFactory()).read(queryPolygonDefinition);
      final QueryConstraints queryConstraints =
          new SpatialTemporalConstraintsBuilderImpl().spatialConstraints(queryPolygon).build();

      // Query the POI adapter
      final Query<POI> query =
          QueryBuilder.newBuilder(POI.class).addTypeName(adapter.getTypeName()).indexName(
              spatialIndex.getName()).constraints(queryConstraints).build();

      System.out.println(
          "Executing query on POI adapter, expecting to match Roman Colosseum and Eiffel Tower...");
      try (final CloseableIterator<POI> iterator = dataStore.query(query)) {
        while (iterator.hasNext()) {
          System.out.println("Query match: " + iterator.next().getName());
        }
      }

      // Now query the annotated POI adapter
      final Query<AnnotatedPOI> annotatedQuery =
          QueryBuilder.newBuilder(AnnotatedPOI.class).addTypeName(
              annotatedAdapter.getTypeName()).indexName(spatialIndex.getName()).constraints(
                  queryConstraints).build();

      System.out.println(
          "Executing query on Annotated POI adapter, expecting to match Roman Colosseum and Eiffel Tower...");
      try (final CloseableIterator<AnnotatedPOI> iterator = dataStore.query(annotatedQuery)) {
        while (iterator.hasNext()) {
          System.out.println("Query match: " + iterator.next().getName());
        }
      }
    } catch (FactoryRegistryException | ParseException e) {
    }

  }

  /**
   * Our custom data type that we want to store inside GeoWave. It contains a name, latitude,
   * longitude, a public string field, and a private string field. Any field that has both an
   * accessor and mutator, or is public will be added to the adapter. Private fields without an
   * accessor and mutator will be ignored.
   */
  public static class POI {
    private String name;
    private Double latitude;
    private Double longitude;
    public String publicField;
    private String privateField = "ignored";

    /**
     * A no-args constructor is required for the `BasicDataTypeAdapter` to create new instances.
     */
    protected POI() {}

    public POI(final String name, final Double latitude, final Double longitude) {
      this.name = name;
      this.latitude = latitude;
      this.longitude = longitude;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setLatitude(final Double latitude) {
      this.latitude = latitude;
    }

    public Double getLatitude() {
      return latitude;
    }

    public void setLongitude(final Double longitude) {
      this.longitude = longitude;
    }

    public Double getLongitude() {
      return longitude;
    }

    public String getPrivateField() {
      return privateField;
    }
  }

  /**
   * Another way to create a data type for the `BasicDataTypeAdapter` is to annotate it with GeoWave
   * field annotations. These annotations provide an additional level of control over the way each
   * field is interpreted. In an annotated class, annotated fields allow the user to specify index
   * hints, alternate field names, and a coordinate reference system for spatial fields.
   * Additionally, the annotated field may be private or final. When using an annotated data type,
   * any non-annotated fields will be ignored. Field annotations will only be used if the class is
   * annotated with `@GeoWaveDataType`.
   */
  @GeoWaveDataType
  public static class AnnotatedPOI {
    @GeoWaveField(name = "alternateFieldName")
    private final String name;

    @GeoWaveTemporalField(timeIndexHint = true)
    private final Date date;

    @GeoWaveSpatialField(latitudeIndexHint = true, crs = "EPSG:4326")
    private final Double latitude;

    @GeoWaveSpatialField(longitudeIndexHint = true, crs = "EPSG:4326")
    private final Double longitude;

    protected AnnotatedPOI() {
      name = null;
      date = null;
      latitude = null;
      longitude = null;
    }

    public AnnotatedPOI(
        final String name,
        final Date date,
        final Double latitude,
        final Double longitude) {
      this.name = name;
      this.date = date;
      this.latitude = latitude;
      this.longitude = longitude;
    }

    public String getName() {
      return name;
    }

    public Double getLatitude() {
      return latitude;
    }

    public Double getLongitude() {
      return longitude;
    }

  }



}
