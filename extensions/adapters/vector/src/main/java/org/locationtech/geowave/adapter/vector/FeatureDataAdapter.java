/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.adapter.TemporalFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.mapreduce.HadoopDataAdapter;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializer;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Lists;

/**
 * This data adapter will handle all reading/writing concerns for storing and retrieving GeoTools
 * SimpleFeature objects to and from a GeoWave persistent store. <br> <br> The adapter will use the
 * SimpleFeature's default geometry for spatial indexing.<br> <br> The adaptor will use the first
 * temporal attribute (a Calendar or Date object) as the timestamp of a temporal index.<br> <br> If
 * the feature type contains a UserData property 'time' for a specific time attribute with
 * Boolean.TRUE, then the attribute is used as the timestamp of a temporal index.<br> <br> If the
 * feature type contains UserData properties 'start' and 'end' for two different time attributes
 * with value Boolean.TRUE, then the attributes are used for a range index.<br> <br> If the feature
 * type contains a UserData property 'time' for *all* time attributes with Boolean.FALSE, then a
 * temporal index is not used.
 */
public class FeatureDataAdapter implements
    GeotoolsFeatureDataAdapter<SimpleFeature>,
    HadoopDataAdapter<SimpleFeature, FeatureWritable>,
    DefaultStatisticsProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDataAdapter.class);
  // the original coordinate system will always be represented internally by
  // the persisted type
  private SimpleFeatureType featureType;

  private TimeDescriptors timeDescriptors = null;
  FieldDescriptor<?>[] fieldDescriptors;
  Map<String, FieldDescriptor<?>> descriptorsMap;

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  protected FeatureDataAdapter() {}

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Constructor<br> Creates a FeatureDataAdapter for the specified SimpleFeatureType
   *
   * @param featureType - feature type for this object
   */
  public FeatureDataAdapter(final SimpleFeatureType featureType) {
    setFeatureType(featureType);
  }

  @Override
  public Class<SimpleFeature> getDataClass() {
    return SimpleFeature.class;
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Set the FeatureType for this Data Adapter.
   *
   * @param featureType - new feature type
   */
  private void setFeatureType(SimpleFeatureType featureType) {
    if (featureType.getCoordinateReferenceSystem() == null) {
      featureType = SimpleFeatureTypeBuilder.retype(featureType, GeometryUtils.getDefaultCRS());
    }
    this.featureType = featureType;
    resetTimeDescriptors();
    initializeFieldDescriptors();
  }

  private void initializeFieldDescriptors() {
    final List<AttributeDescriptor> attributes = featureType.getAttributeDescriptors();
    fieldDescriptors = new FieldDescriptor[attributes.size()];

    for (int i = 0; i < attributes.size(); i++) {
      final AttributeDescriptor attribute = attributes.get(i);
      if (attribute instanceof GeometryDescriptor) {
        final SpatialFieldDescriptorBuilder<?> builder =
            new SpatialFieldDescriptorBuilder<>(attribute.getType().getBinding());
        builder.fieldName(attribute.getName().getLocalPart());
        builder.crs(((GeometryDescriptor) attribute).getCoordinateReferenceSystem());
        if ((featureType.getGeometryDescriptor() != null)
            && featureType.getGeometryDescriptor().equals(attribute)) {
          builder.spatialIndexHint();
        }
        fieldDescriptors[i] = builder.build();
      } else if ((timeDescriptors != null) && attribute.equals(timeDescriptors.getTime())) {
        fieldDescriptors[i] =
            new TemporalFieldDescriptorBuilder<>(attribute.getType().getBinding()).fieldName(
                attribute.getName().getLocalPart()).timeIndexHint().build();
      } else if ((timeDescriptors != null) && attribute.equals(timeDescriptors.getStartRange())) {
        fieldDescriptors[i] =
            new TemporalFieldDescriptorBuilder<>(attribute.getType().getBinding()).fieldName(
                attribute.getName().getLocalPart()).startTimeIndexHint().build();
      } else if ((timeDescriptors != null) && attribute.equals(timeDescriptors.getEndRange())) {
        fieldDescriptors[i] =
            new TemporalFieldDescriptorBuilder<>(attribute.getType().getBinding()).fieldName(
                attribute.getName().getLocalPart()).endTimeIndexHint().build();
      } else {
        fieldDescriptors[i] =
            new FieldDescriptorBuilder<>(attribute.getType().getBinding()).fieldName(
                attribute.getName().getLocalPart()).build();
      }
    }

    // this assumes attribute names are unique, which *should* be a fair assumption
    descriptorsMap =
        Arrays.stream(fieldDescriptors).collect(
            Collectors.toMap(FieldDescriptor::fieldName, descriptor -> descriptor));
  }

  /**
   * Sets the namespace of the reprojected feature type associated with this data adapter
   *
   * @param namespaceURI - new namespace URI
   */
  @Override
  public void setNamespace(final String namespaceURI) {
    final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    builder.init(featureType);
    builder.setNamespaceURI(namespaceURI);
    featureType = builder.buildFeatureType();
  }

  // ----------------------------------------------------------------------------------
  /** Map of Field Readers associated with a Field ID */
  private final Map<String, FieldReader<Object>> mapOfFieldNameToReaders = new HashMap<>();

  /**
   * {@inheritDoc}
   *
   * @return Field Reader for the given Field ID
   */
  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    // Go to the map to get a reader for given fieldId

    FieldReader<Object> reader = mapOfFieldNameToReaders.get(fieldName);

    // Check the map to see if a reader has already been found.
    if (reader == null) {
      // Reader not in Map, go to the reprojected feature type and get the
      // default reader
      final AttributeDescriptor descriptor = featureType.getDescriptor(fieldName);
      final Class<?> bindingClass = descriptor.getType().getBinding();
      reader = (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(bindingClass);

      // Add it to map for the next time
      mapOfFieldNameToReaders.put(fieldName, reader);
    }

    return reader;
  }

  // ----------------------------------------------------------------------------------
  /** Map of Field Writers associated with a Field ID */
  private final Map<String, FieldWriter<Object>> mapOfFieldNameToWriters = new HashMap<>();

  /**
   * {@inheritDoc}
   *
   * @return Field Writer for the given Field ID
   */
  @Override
  public FieldWriter<Object> getWriter(final String fieldName) {
    // Go to the map to get a writer for given fieldId

    FieldWriter<Object> writer = mapOfFieldNameToWriters.get(fieldName);

    // Check the map to see if a writer has already been found.
    if (writer == null) {
      final AttributeDescriptor descriptor = featureType.getDescriptor(fieldName);

      final Class<?> bindingClass = descriptor.getType().getBinding();
      writer = (FieldWriter<Object>) FieldUtils.getDefaultWriterForClass(bindingClass);
      if (writer == null) {
        LOGGER.error("BasicWriter not found for binding type:" + bindingClass.getName().toString());
      }

      mapOfFieldNameToWriters.put(fieldName, writer);
    }
    return writer;
  }

  @Override
  public String getTypeName() {
    return featureType.getTypeName();
  }

  @Override
  public byte[] getDataId(final SimpleFeature entry) {
    return StringUtils.stringToBinary(entry.getID());
  }

  @Override
  public RowBuilder<SimpleFeature> newRowBuilder(
      final FieldDescriptor<?>[] outputFieldDescriptors) {
    CoordinateReferenceSystem outputCRS = featureType.getCoordinateReferenceSystem();
    final String defaultGeometryField = featureType.getGeometryDescriptor().getLocalName();
    for (final FieldDescriptor<?> field : outputFieldDescriptors) {
      if (field.fieldName().equals(defaultGeometryField)
          && (field instanceof SpatialFieldDescriptor)) {
        outputCRS = ((SpatialFieldDescriptor<?>) field).crs();
        break;
      }
    }

    CoordinateReferenceSystem persistedCRS = featureType.getCoordinateReferenceSystem();

    if (outputCRS == null) {
      outputCRS = GeometryUtils.getDefaultCRS();
    }

    if (persistedCRS == null) {
      persistedCRS = GeometryUtils.getDefaultCRS();
    }

    final SimpleFeatureType reprojectedFeatureType;
    if (outputCRS.equals(persistedCRS)) {
      reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(featureType, persistedCRS);
    } else {
      reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(featureType, outputCRS);
    }
    return new FeatureRowBuilder(reprojectedFeatureType);
  }

  @Override
  public SimpleFeatureType getFeatureType() {
    return featureType;
  }

  @Override
  public boolean hasTemporalConstraints() {
    return getTimeDescriptors().hasTime();
  }

  public synchronized void resetTimeDescriptors() {
    timeDescriptors = TimeUtils.inferTimeAttributeDescriptor(featureType);
  }

  @Override
  public synchronized TimeDescriptors getTimeDescriptors() {
    if (timeDescriptors == null) {
      timeDescriptors = TimeUtils.inferTimeAttributeDescriptor(featureType);
    }
    return timeDescriptors;
  }

  @Override
  public HadoopWritableSerializer<SimpleFeature, FeatureWritable> createWritableSerializer() {
    return new FeatureWritableSerializer(featureType);
  }

  private static class FeatureWritableSerializer implements
      HadoopWritableSerializer<SimpleFeature, FeatureWritable> {
    private final FeatureWritable writable;

    FeatureWritableSerializer(final SimpleFeatureType type) {
      writable = new FeatureWritable(type);
    }

    @Override
    public FeatureWritable toWritable(final SimpleFeature entry) {
      writable.setFeature(entry);
      return writable;
    }

    @Override
    public SimpleFeature fromWritable(final FeatureWritable writable) {
      return writable.getFeature();
    }
  }

  @Override
  public Object getFieldValue(final SimpleFeature entry, final String fieldName) {
    return entry.getAttribute(fieldName);
  }

  public static CoordinateReferenceSystem decodeCRS(final String crsCode) {

    CoordinateReferenceSystem crs = null;
    try {
      crs = CRS.decode(crsCode, true);
    } catch (final FactoryException e) {
      LOGGER.error("Unable to decode '" + crsCode + "' CRS", e);
      throw new RuntimeException("Unable to initialize '" + crsCode + "' object", e);
    }

    return crs;
  }

  @Override
  public List<Statistic<? extends StatisticValue<?>>> getDefaultStatistics() {
    final List<Statistic<?>> statistics = Lists.newArrayList();
    final CountStatistic count = new CountStatistic(getTypeName());
    count.setInternal();
    statistics.add(count);
    for (int i = 0; i < featureType.getAttributeCount(); i++) {
      final AttributeDescriptor ad = featureType.getDescriptor(i);
      if (Geometry.class.isAssignableFrom(ad.getType().getBinding())) {
        final BoundingBoxStatistic bbox =
            new BoundingBoxStatistic(getTypeName(), ad.getLocalName());
        bbox.setInternal();
        statistics.add(bbox);
      }
    }
    final TimeDescriptors timeDescriptors = getTimeDescriptors();
    if (timeDescriptors.hasTime()) {
      if (timeDescriptors.getTime() != null) {
        final TimeRangeStatistic timeRange =
            new TimeRangeStatistic(getTypeName(), timeDescriptors.getTime().getLocalName());
        timeRange.setInternal();
        statistics.add(timeRange);
      }
      if (timeDescriptors.getStartRange() != null) {
        final TimeRangeStatistic timeRange =
            new TimeRangeStatistic(getTypeName(), timeDescriptors.getStartRange().getLocalName());
        timeRange.setInternal();
        statistics.add(timeRange);
      }
      if (timeDescriptors.getEndRange() != null) {
        final TimeRangeStatistic timeRange =
            new TimeRangeStatistic(getTypeName(), timeDescriptors.getEndRange().getLocalName());
        timeRange.setInternal();
        statistics.add(timeRange);
      }
    }
    return statistics;
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return fieldDescriptors;
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
    return descriptorsMap.get(fieldName);
  }

  @Override
  public byte[] toBinary() {
    // serialize the persisted/reprojected feature type by using default
    // fields and
    // data types

    final String encodedType = DataUtilities.encodeType(featureType);
    final String axis = FeatureDataUtils.getAxis(featureType.getCoordinateReferenceSystem());
    final String typeName = featureType.getTypeName();
    final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
    final byte[] axisBytes = StringUtils.stringToBinary(axis);
    //
    final SimpleFeatureUserDataConfigurationSet userDataConfiguration =
        new SimpleFeatureUserDataConfigurationSet();
    userDataConfiguration.addConfigurations(typeName, new TimeDescriptorConfiguration(featureType));
    final byte[] attrBytes = userDataConfiguration.toBinary();
    final String namespace = featureType.getName().getNamespaceURI();

    byte[] namespaceBytes;
    if ((namespace != null) && (namespace.length() > 0)) {
      namespaceBytes = StringUtils.stringToBinary(namespace);
    } else {
      namespaceBytes = new byte[0];
    }
    final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
    // 21 bytes is the 7 four byte length fields and one byte for the
    // version
    final ByteBuffer buf =
        ByteBuffer.allocate(
            encodedTypeBytes.length
                + typeNameBytes.length
                + namespaceBytes.length
                + attrBytes.length
                + axisBytes.length
                + VarintUtils.unsignedIntByteLength(typeNameBytes.length)
                + VarintUtils.unsignedIntByteLength(namespaceBytes.length)
                + VarintUtils.unsignedIntByteLength(attrBytes.length)
                + VarintUtils.unsignedIntByteLength(axisBytes.length)
                + VarintUtils.unsignedIntByteLength(encodedTypeBytes.length));
    VarintUtils.writeUnsignedInt(typeNameBytes.length, buf);
    VarintUtils.writeUnsignedInt(namespaceBytes.length, buf);
    VarintUtils.writeUnsignedInt(attrBytes.length, buf);
    VarintUtils.writeUnsignedInt(axisBytes.length, buf);
    VarintUtils.writeUnsignedInt(encodedTypeBytes.length, buf);
    buf.put(typeNameBytes);
    buf.put(namespaceBytes);
    buf.put(attrBytes);
    buf.put(axisBytes);
    buf.put(encodedTypeBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    GeometryUtils.initClassLoader();
    // deserialize the feature type
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int typeNameByteLength = VarintUtils.readUnsignedInt(buf);
    final int namespaceByteLength = VarintUtils.readUnsignedInt(buf);

    final int attrByteLength = VarintUtils.readUnsignedInt(buf);
    final int axisByteLength = VarintUtils.readUnsignedInt(buf);
    final int encodedTypeByteLength = VarintUtils.readUnsignedInt(buf);

    final byte[] typeNameBytes = ByteArrayUtils.safeRead(buf, typeNameByteLength);
    final byte[] namespaceBytes = ByteArrayUtils.safeRead(buf, namespaceByteLength);
    final byte[] attrBytes = ByteArrayUtils.safeRead(buf, attrByteLength);
    final byte[] axisBytes = ByteArrayUtils.safeRead(buf, axisByteLength);
    final byte[] encodedTypeBytes = ByteArrayUtils.safeRead(buf, encodedTypeByteLength);

    final String typeName = StringUtils.stringFromBinary(typeNameBytes);
    String namespace = StringUtils.stringFromBinary(namespaceBytes);
    if (namespace.length() == 0) {
      namespace = null;
    }

    // 21 bytes is the 7 four byte length fields and one byte for the
    // version
    final byte[] secondaryIndexBytes = new byte[buf.remaining()];
    buf.get(secondaryIndexBytes);

    final String encodedType = StringUtils.stringFromBinary(encodedTypeBytes);
    try {
      final SimpleFeatureType myType =
          FeatureDataUtils.decodeType(
              namespace,
              typeName,
              encodedType,
              StringUtils.stringFromBinary(axisBytes));

      final SimpleFeatureUserDataConfigurationSet userDataConfiguration =
          new SimpleFeatureUserDataConfigurationSet();
      userDataConfiguration.fromBinary(attrBytes);
      userDataConfiguration.updateType(myType);
      setFeatureType(myType);

    } catch (final SchemaException e) {
      LOGGER.error("Unable to deserialized feature type", e);
    }

  }

}
