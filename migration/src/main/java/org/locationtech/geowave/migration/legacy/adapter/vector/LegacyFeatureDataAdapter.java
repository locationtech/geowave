/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.legacy.adapter.vector;

import java.nio.ByteBuffer;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.JsonFieldLevelVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LegacyFeatureDataAdapter implements DataTypeAdapter<SimpleFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyFeatureDataAdapter.class);

  private SimpleFeatureType persistedFeatureType;
  private SimpleFeatureType reprojectedFeatureType;
  private FeatureDataAdapter updatedAdapter;

  public LegacyFeatureDataAdapter() {}

  /**
   * The legacy feature data adapter stored an index CRS code, which we need for testing migration.
   */
  public LegacyFeatureDataAdapter(final SimpleFeatureType featureType, final String indexCRSCode) {
    persistedFeatureType = featureType;
    initCRS(indexCRSCode);
  }

  private void initCRS(String indexCrsCode) {
    if ((indexCrsCode == null) || indexCrsCode.isEmpty()) {
      indexCrsCode = GeometryUtils.DEFAULT_CRS_STR;
    }
    CoordinateReferenceSystem persistedCRS = persistedFeatureType.getCoordinateReferenceSystem();

    if (persistedCRS == null) {
      persistedCRS = GeometryUtils.getDefaultCRS();
    }

    final CoordinateReferenceSystem indexCRS = decodeCRS(indexCrsCode);
    if (indexCRS.equals(persistedCRS)) {
      reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(persistedFeatureType, persistedCRS);
    } else {
      reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(persistedFeatureType, indexCRS);
    }
  }

  private CoordinateReferenceSystem decodeCRS(final String crsCode) {

    CoordinateReferenceSystem crs = null;
    try {
      crs = CRS.decode(crsCode, true);
    } catch (final FactoryException e) {
      LOGGER.error("Unable to decode '" + crsCode + "' CRS", e);
      throw new RuntimeException("Unable to initialize '" + crsCode + "' object", e);
    }

    return crs;
  }

  public FeatureDataAdapter getUpdatedAdapter() {
    return updatedAdapter;
  }

  public VisibilityHandler getVisibilityHandler() {
    VisibilityHandler visibilityHandler = new UnconstrainedVisibilityHandler();
    for (final AttributeDescriptor attrDesc : persistedFeatureType.getAttributeDescriptors()) {
      if (attrDesc.getUserData().containsKey("visibility")
          && Boolean.TRUE.equals(attrDesc.getUserData().get("visibility"))) {
        final Object visMgr = persistedFeatureType.getUserData().get("visibilityManagerClass");
        if (visMgr == null) {
          // If no visibility manager is present, then can't configure
          break;
        }
        if (visMgr.toString().equals(
            "org.locationtech.geowave.adapter.vector.plugin.visibility.JsonDefinitionColumnVisibilityManagement")
            || visMgr.toString().equals(
                "org.locationtech.geowave.adapter.vector.plugin.visibility.VisibilityConfiguration")) {
          // Pre 2.0, this was the only configurable visibility manager supported by GeoWave
          visibilityHandler = new JsonFieldLevelVisibilityHandler(attrDesc.getLocalName());
        } else {
          // Custom visibility management classes can't be migrated
          LOGGER.warn(
              "Custom visibility manager '"
                  + visMgr
                  + "' is not supported by the migration, a default unconstrained visibility handler will be used.");
        }
      }
    }
    return visibilityHandler;
  }

  public SimpleFeatureType getFeatureType() {
    return persistedFeatureType;
  }

  @Override
  public byte[] toBinary() {
    final String encodedType = DataUtilities.encodeType(persistedFeatureType);
    final String axis =
        FeatureDataUtils.getAxis(persistedFeatureType.getCoordinateReferenceSystem());
    final String typeName = reprojectedFeatureType.getTypeName();
    final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
    final byte[] axisBytes = StringUtils.stringToBinary(axis);
    //
    final SimpleFeatureUserDataConfigurationSet userDataConfiguration =
        new SimpleFeatureUserDataConfigurationSet();
    userDataConfiguration.addConfigurations(
        typeName,
        new TimeDescriptorConfiguration(persistedFeatureType));
    userDataConfiguration.addConfigurations(
        typeName,
        new LegacyVisibilityConfiguration(persistedFeatureType));
    final byte[] attrBytes = userDataConfiguration.toBinary();
    final String namespace = reprojectedFeatureType.getName().getNamespaceURI();

    byte[] namespaceBytes;
    if ((namespace != null) && (namespace.length() > 0)) {
      namespaceBytes = StringUtils.stringToBinary(namespace);
    } else {
      namespaceBytes = new byte[0];
    }
    final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
    final CoordinateReferenceSystem crs = reprojectedFeatureType.getCoordinateReferenceSystem();
    final byte[] indexCrsBytes;
    if (crs != null) {
      indexCrsBytes = StringUtils.stringToBinary(CRS.toSRS(crs));
    } else {
      indexCrsBytes = new byte[0];
    }
    // 21 bytes is the 7 four byte length fields and one byte for the
    // version
    ByteBuffer buf =
        ByteBuffer.allocate(
            encodedTypeBytes.length
                + indexCrsBytes.length
                + typeNameBytes.length
                + namespaceBytes.length
                + attrBytes.length
                + axisBytes.length
                + VarintUtils.unsignedIntByteLength(typeNameBytes.length)
                + VarintUtils.unsignedIntByteLength(indexCrsBytes.length)
                + VarintUtils.unsignedIntByteLength(namespaceBytes.length)
                + VarintUtils.unsignedIntByteLength(attrBytes.length)
                + VarintUtils.unsignedIntByteLength(axisBytes.length)
                + VarintUtils.unsignedIntByteLength(encodedTypeBytes.length));
    VarintUtils.writeUnsignedInt(typeNameBytes.length, buf);
    VarintUtils.writeUnsignedInt(indexCrsBytes.length, buf);
    VarintUtils.writeUnsignedInt(namespaceBytes.length, buf);
    VarintUtils.writeUnsignedInt(attrBytes.length, buf);
    VarintUtils.writeUnsignedInt(axisBytes.length, buf);
    VarintUtils.writeUnsignedInt(encodedTypeBytes.length, buf);
    buf.put(typeNameBytes);
    buf.put(indexCrsBytes);
    buf.put(namespaceBytes);
    buf.put(attrBytes);
    buf.put(axisBytes);
    buf.put(encodedTypeBytes);

    final byte[] defaultTypeDataBinary = buf.array();
    final byte[] persistablesBytes = new byte[0]; // We won't be using or reading any of the
                                                  // original persistables
    buf =
        ByteBuffer.allocate(
            defaultTypeDataBinary.length
                + persistablesBytes.length
                + VarintUtils.unsignedIntByteLength(defaultTypeDataBinary.length));
    VarintUtils.writeUnsignedInt(defaultTypeDataBinary.length, buf);
    buf.put(defaultTypeDataBinary);
    buf.put(persistablesBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    GeometryUtils.initClassLoader();
    // deserialize the feature type
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int defaultTypeDataBinaryLength = VarintUtils.readUnsignedInt(buf);
    if (defaultTypeDataBinaryLength > 0) {
      final byte[] defaultTypeDataBinary =
          ByteArrayUtils.safeRead(buf, defaultTypeDataBinaryLength);
      buf = ByteBuffer.wrap(defaultTypeDataBinary);
      final int typeNameByteLength = VarintUtils.readUnsignedInt(buf);
      final int indexCrsByteLength = VarintUtils.readUnsignedInt(buf);
      final int namespaceByteLength = VarintUtils.readUnsignedInt(buf);

      final int attrByteLength = VarintUtils.readUnsignedInt(buf);
      final int axisByteLength = VarintUtils.readUnsignedInt(buf);
      final int encodedTypeByteLength = VarintUtils.readUnsignedInt(buf);

      final byte[] typeNameBytes = ByteArrayUtils.safeRead(buf, typeNameByteLength);
      // We don't need this anymore
      ByteArrayUtils.safeRead(buf, indexCrsByteLength);
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
        persistedFeatureType = myType;
        updatedAdapter = new FeatureDataAdapter(myType);
      } catch (final SchemaException e) {
        LOGGER.error("Unable to deserialized feature type", e);
      }
    }
  }

  @Override
  public String getTypeName() {
    return updatedAdapter.getTypeName();
  }

  @Override
  public byte[] getDataId(final SimpleFeature entry) {
    return updatedAdapter.getDataId(entry);
  }

  @Override
  public Object getFieldValue(final SimpleFeature entry, final String fieldName) {
    return updatedAdapter.getFieldValue(entry, fieldName);
  }

  @Override
  public Class<SimpleFeature> getDataClass() {
    return updatedAdapter.getDataClass();
  }

  @Override
  public RowBuilder<SimpleFeature> newRowBuilder(
      final FieldDescriptor<?>[] outputFieldDescriptors) {
    return updatedAdapter.newRowBuilder(outputFieldDescriptors);
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return updatedAdapter.getFieldDescriptors();
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
    return updatedAdapter.getFieldDescriptor(fieldName);
  }

}
