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
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.factory.FactoryRegistryException;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.store.query.SpatialTemporalConstraintsBuilderImpl;
import org.locationtech.geowave.core.geotime.store.query.api.SpatialTemporalConstraintsBuilder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AbstractDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * This class provides an example of how to create a {@link DataTypeAdapter} for a custom data type.
 * This allows a user to directly write, index, and query their own data types from a GeoWave data
 * store without having to translate to and from a `SimpleFeature`. A custom data adapter
 * implementation may be useful for data types that are too complex for the
 * {@link BasicDataTypeAdapter}, such as when annotations cannot be added or the fields cannot be
 * properly inferred.
 */
public class CustomAdapterExample {

  private DataStore dataStore;
  private DataTypeAdapter<POI> adapter;
  private Index spatialIndex;

  public static void main(final String[] args) throws IOException, CQLException {

    final CustomAdapterExample example = new CustomAdapterExample();
    example.run();
  }

  public void run() {
    // Create an in-memory data store to use with this example
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    // Create our custom adapter with the type name `POI`
    adapter = new POIBasicDataAdapter("POI");

    // Create the spatial index
    spatialIndex = new SpatialIndexBuilder().createIndex();

    // Add the type to the data store with the spatial index
    dataStore.addType(adapter, spatialIndex);

    // Ingest the data into a spatial index
    ingestData();

    // Perform a spatial query on the data
    querySpatial();
  }

  private void ingestData() {
    try (Writer<POI> writer = dataStore.createWriter(adapter.getTypeName())) {
      // With our custom adapter, we can directly write `POI` instances to the data store
      writer.write(new POI("Eiffel Tower", 48.858093, 2.294694));
      writer.write(new POI("Roman Colosseum", 41.890167, 12.492269));
      writer.write(new POI("Great Pyramid of Giza", 29.979176, 31.134357));
      writer.write(new POI("Mount Everest", 27.986065, 86.922623));
    }
  }

  private void querySpatial() {
    // Because we have hinted to GeoWave that our type contains spatial data, we can utilize spatial
    // constraints when querying
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
      final SpatialTemporalConstraintsBuilder spatialConstraintsBuilder =
          new SpatialTemporalConstraintsBuilderImpl();
      final Query<POI> query =
          QueryBuilder.newBuilder(POI.class).addTypeName(adapter.getTypeName()).indexName(
              spatialIndex.getName()).constraints(
                  spatialConstraintsBuilder.spatialConstraints(queryPolygon).build()).build();

      System.out.println("Executing query, expecting to match Roman Colosseum and Eiffel Tower...");
      try (final CloseableIterator<POI> iterator = dataStore.query(query)) {
        while (iterator.hasNext()) {
          System.out.println("Query match: " + iterator.next().getName());
        }
      }
    } catch (FactoryRegistryException | ParseException e) {
    }

  }

  /**
   * Our custom data type that we want to store inside GeoWave. It contains a name, latitude, and
   * longitude.
   */
  public static class POI {
    private final String name;
    private final Double latitude;
    private final Double longitude;

    public POI(final String name, final Double latitude, final Double longitude) {
      this.name = name;
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

  /**
   * The simplest way to implement a data adapter for a custom data type is to extend the
   * {@link AbstractDataTypeAdapter} and implement the methods that read and write the custom type.
   * It's important to note that any adapter that extends the `AbstractDataTypeAdapter` must be
   * added to the persistable registry.
   */
  public static class POIBasicDataAdapter extends AbstractDataTypeAdapter<POI> {
    public static final String NAME_FIELD_NAME = "name";
    public static final String LATITUDE_FIELD_NAME = "lat";
    public static final String LONGITUDE_FIELD_NAME = "lon";

    // We create a field descriptor for each field in our data type to tell GeoWave how to handle
    // the data. For the latitude and longitude fields, we provide index hints that identify those
    // fields as such, as well as a `CoordinateReferenceSystem` so that our type will be properly
    // transformed if the index has a different CRS.
    private static final FieldDescriptor<String> NAME_FIELD =
        new FieldDescriptorBuilder<>(String.class).fieldName(NAME_FIELD_NAME).build();
    private static final FieldDescriptor<Double> LATITUDE_FIELD =
        new SpatialFieldDescriptorBuilder<>(Double.class).fieldName(LATITUDE_FIELD_NAME).crs(
            DefaultGeographicCRS.WGS84).latitudeIndexHint().build();
    private static final FieldDescriptor<Double> LONGITUDE_FIELD =
        new SpatialFieldDescriptorBuilder<>(Double.class).fieldName(LONGITUDE_FIELD_NAME).crs(
            DefaultGeographicCRS.WGS84).longitudeIndexHint().build();
    private static final FieldDescriptor<?>[] FIELDS =
        new FieldDescriptor[] {NAME_FIELD, LATITUDE_FIELD, LONGITUDE_FIELD};

    public POIBasicDataAdapter() {}

    public POIBasicDataAdapter(final String typeName) {
      super(typeName, FIELDS, NAME_FIELD);
    }

    @Override
    public Object getFieldValue(final POI entry, final String fieldName) {
      switch (fieldName) {
        case NAME_FIELD_NAME:
          return entry.name;
        case LATITUDE_FIELD_NAME:
          return entry.latitude;
        case LONGITUDE_FIELD_NAME:
          return entry.longitude;
      }
      return null;
    }

    @Override
    public POI buildObject(final Object dataId, final Object[] fieldValues) {
      return new POI((String) fieldValues[0], (Double) fieldValues[1], (Double) fieldValues[2]);
    }

  }

}
