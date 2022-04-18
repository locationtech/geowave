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
import org.locationtech.geowave.core.geotime.util.SimpleFeatureUserDataConfiguration;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * Describes which attribute in a feature contains the visibility constraints, interpreted by a
 * {@link ColumnVisibilityManagementSpi}
 */
public class LegacyVisibilityConfiguration implements SimpleFeatureUserDataConfiguration {

  private static final long serialVersionUID = -664252700036603897L;
  private String attributeName = "GEOWAVE_VISIBILITY";
  private String managerClassName = "";

  public LegacyVisibilityConfiguration() {}

  public LegacyVisibilityConfiguration(final SimpleFeatureType persistType) {
    configureFromType(persistType);
  }

  /**
   * {@inheritDoc} Method that updates visibility for the passed in SimpleFeatureType.
   *
   * @param persistType - type object to be updated
   */
  @Override
  public void updateType(final SimpleFeatureType persistType) {
    // First, remove the visibility UserData from all attributes
    for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
      attrDesc.getUserData().remove("visibility");
    }

    final AttributeDescriptor attrDesc = persistType.getDescriptor(attributeName);
    if (attrDesc != null) {
      attrDesc.getUserData().put("visibility", Boolean.TRUE);
    }

    persistType.getUserData().put("visibilityManagerClass", managerClassName);
  }

  @Override
  public void configureFromType(final SimpleFeatureType persistType) {
    for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
      if (attrDesc.getUserData().containsKey("visibility")
          && Boolean.TRUE.equals(attrDesc.getUserData().get("visibility"))) {
        final Object visMgr = persistType.getUserData().get("visibilityManagerClass");
        if (visMgr == null) {
          // If no visibility manager is present, then can't configure
          break;
        }
        attributeName = attrDesc.getLocalName();
        managerClassName = visMgr.toString();
      }
    }
  }

  @Override
  public byte[] toBinary() {
    byte[] managerClassBytes;
    if (managerClassName != null) {
      managerClassBytes = StringUtils.stringToBinary(managerClassName);
    } else {
      managerClassBytes = new byte[0];
    }
    byte[] attributeBytes;
    if (attributeName != null) {
      attributeBytes = StringUtils.stringToBinary(attributeName);
    } else {
      attributeBytes = new byte[0];
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            attributeBytes.length
                + managerClassBytes.length
                + VarintUtils.unsignedIntByteLength(attributeBytes.length)
                + VarintUtils.unsignedIntByteLength(managerClassBytes.length));
    VarintUtils.writeUnsignedInt(attributeBytes.length, buf);
    buf.put(attributeBytes);
    VarintUtils.writeUnsignedInt(managerClassBytes.length, buf);
    buf.put(managerClassBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int attributeBytesLength = VarintUtils.readUnsignedInt(buf);
    if (attributeBytesLength > 0) {
      final byte[] attributeBytes = ByteArrayUtils.safeRead(buf, attributeBytesLength);
      attributeName = StringUtils.stringFromBinary(attributeBytes);
    } else {
      attributeName = null;
    }
    final int managerClassBytesLength = VarintUtils.readUnsignedInt(buf);
    if (managerClassBytesLength > 0) {
      final byte[] managerClassBytes = ByteArrayUtils.safeRead(buf, managerClassBytesLength);
      managerClassName = StringUtils.stringFromBinary(managerClassBytes);
    } else {
      managerClassName = null;
    }
  }
}
