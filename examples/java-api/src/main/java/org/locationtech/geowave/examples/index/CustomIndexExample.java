/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.index;

import java.io.IOException;
import java.util.UUID;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import com.google.common.collect.Lists;

/**
 * This class is intended to provide a self-contained, easy-to-follow example of how a custom index
 * can be created. In this example, we will create a UUID index that can be used alongside a spatial
 * index in order to efficiently query features by a String UUID field that each feature has.
 */
public class CustomIndexExample {
  private DataStore dataStore;
  private SimpleFeatureType simpleFeatureType;
  private FeatureDataAdapter adapter;
  private Index spatialIndex;
  private Index customIndex;

  private final String uuid1 = UUID.randomUUID().toString();
  private final String uuid2 = UUID.randomUUID().toString();
  private final String uuid3 = UUID.randomUUID().toString();
  private final String uuid4 = UUID.randomUUID().toString();

  public static void main(final String[] args) throws IOException, CQLException {

    final CustomIndexExample example = new CustomIndexExample();
    example.run();
  }

  public void run() {
    // Create an in-memory data store to use with this example
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    // Create the simple feature type for our data
    simpleFeatureType = getSimpleFeatureType();

    // Create an adapter for our features
    adapter = new FeatureDataAdapter(simpleFeatureType);

    // Create the spatial index
    spatialIndex = new SpatialIndexBuilder().createIndex();

    // Create our custom index using the UUID index strategy
    customIndex = new CustomIndex<>(new UUIDIndexStrategy("uuid"), "customIdx");

    // Add the type to the data store with the spatial and custom indices
    dataStore.addType(adapter, spatialIndex, customIndex);

    // Ingest the data into a spatial index and our custom index
    ingestData();

    // Perform a spatial query on the data
    querySpatial();

    // Perform a UUID query on the data
    queryUUID();
  }

  public void ingestData() {
    try (Writer<SimpleFeature> writer = dataStore.createWriter(adapter.getTypeName())) {
      writer.write(buildSimpleFeature("feature1", new Coordinate(0, 0), uuid1));
      writer.write(buildSimpleFeature("feature2", new Coordinate(1, 1), uuid2));
      writer.write(buildSimpleFeature("feature3", new Coordinate(2, 2), uuid3));
      writer.write(buildSimpleFeature("feature4", new Coordinate(3, 3), uuid4));

      // Entries with the same UUID will be placed next to each other in the index
      writer.write(buildSimpleFeature("feature5", new Coordinate(4, 4), uuid2));
    }

  }

  public void querySpatial() {
    System.out.println("Executing query, expecting to match feature2, feature3, and feature4...");
    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    try (final CloseableIterator<SimpleFeature> iterator =
        dataStore.query(
            bldr.indexName(spatialIndex.getName()).addTypeName(adapter.getTypeName()).constraints(
                bldr.constraintsFactory().cqlConstraints(
                    "BBOX(geometry,0.5,0.5,3.5,3.5)")).build())) {

      while (iterator.hasNext()) {
        System.out.println("Query match: " + iterator.next().getID());
      }
    }
  }

  public void queryUUID() {
    System.out.println("Executing query, expecting to match feature1 with UUID [" + uuid1 + "]...");
    VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    // When querying our custom index, we can provide our custom constraints by using the
    // customConstraints function on the constraints factory.
    try (final CloseableIterator<SimpleFeature> iterator =
        dataStore.query(
            bldr.indexName(customIndex.getName()).addTypeName(adapter.getTypeName()).constraints(
                bldr.constraintsFactory().customConstraints(new UUIDConstraints(uuid1))).build())) {

      while (iterator.hasNext()) {
        System.out.println("Query match: " + iterator.next().getID());
      }
    }

    System.out.println(
        "Executing query, expecting to match feature2 and feature5 with UUID [" + uuid2 + "]...");
    bldr = VectorQueryBuilder.newBuilder();
    try (final CloseableIterator<SimpleFeature> iterator =
        dataStore.query(
            bldr.indexName(customIndex.getName()).addTypeName(adapter.getTypeName()).constraints(
                bldr.constraintsFactory().customConstraints(new UUIDConstraints(uuid2))).build())) {

      while (iterator.hasNext()) {
        System.out.println("Query match: " + iterator.next().getID());
      }
    }
  }

  private SimpleFeatureType getSimpleFeatureType() {
    final String NAME = "ExampleType";
    final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();
    sftBuilder.setName(NAME);
    sftBuilder.add(atBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
    sftBuilder.add(atBuilder.binding(String.class).nillable(false).buildDescriptor("uuid"));

    return sftBuilder.buildFeatureType();
  }

  private SimpleFeature buildSimpleFeature(
      final String featureId,
      final Coordinate coordinate,
      final String uuid) {

    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(simpleFeatureType);
    builder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));
    builder.set("uuid", uuid);

    return builder.buildFeature(featureId);
  }

  /**
   * This index strategy will index data by using an attribute of a simple feature as the sort key
   * in the index. This implementation allows the user to supply the field name for the UUID field
   * to offer some flexibility.
   */
  public static class UUIDIndexStrategy implements
      CustomIndexStrategy<SimpleFeature, UUIDConstraints> {

    private String uuidField;

    public UUIDIndexStrategy() {}

    public UUIDIndexStrategy(final String uuidField) {
      this.uuidField = uuidField;
    }

    /**
     * Store any data needed to persist this index strategy.
     */
    @Override
    public byte[] toBinary() {
      return StringUtils.stringToBinary(uuidField);
    }

    /**
     * Load the index strategy UUID field from binary.
     */
    @Override
    public void fromBinary(final byte[] bytes) {
      uuidField = StringUtils.stringFromBinary(bytes);
    }

    /**
     * The method supplies all of the insertion IDs needed for a given entry. It is possible to
     * insert the same SimpleFeature multiple times in the index under different insertion IDs, but
     * for this case we only need to use the UUID as the lone insertion ID.
     *
     * @param entry the feature to generate sort keys for.
     * @return the insertion IDs for the given feature
     */
    @Override
    public InsertionIds getInsertionIds(final SimpleFeature entry) {
      final String featureUUID = (String) entry.getAttribute(uuidField);
      return new InsertionIds(Lists.newArrayList(StringUtils.stringToBinary(featureUUID)));
    }

    /**
     * This method generates the query ranges to be used by the data store implementation to
     * retrieve features from the database. For this example, we are only interested in querying for
     * an exact UUID, so we can simply use the desired UUID as the query range.
     */
    @Override
    public QueryRanges getQueryRanges(final UUIDConstraints constraints) {
      final byte[] sortKey = StringUtils.stringToBinary(constraints.uuid());
      return new QueryRanges(new ByteArrayRange(sortKey, sortKey));
    }

    @Override
    public Class<UUIDConstraints> getConstraintsClass() {
      return UUIDConstraints.class;
    }

  }

  /**
   * This class serves as constraints for our UUID index strategy. Since we only need to query for
   * exact UUIDs, the constraints class is fairly straightforward. We only need a single UUID String
   * to use as our constraint.
   */
  public static class UUIDConstraints implements Persistable {
    private String uuid;

    public UUIDConstraints() {}

    public UUIDConstraints(final String uuid) {
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
    }

    /**
     * Serialize any data needed to persist this constraint.
     */
    @Override
    public byte[] toBinary() {
      return StringUtils.stringToBinary(uuid);
    }

    /**
     * Load the UUID constraint from binary.
     */
    @Override
    public void fromBinary(final byte[] bytes) {
      uuid = StringUtils.stringFromBinary(bytes);

    }

  }
}
