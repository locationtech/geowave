/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.index;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.index.SecondaryIndexType;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSecondaryIndexConfiguration<T>
    implements SimpleFeatureSecondaryIndexConfiguration {

  private static final long serialVersionUID = -7425830022998223202L;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSecondaryIndexConfiguration.class);
  private Class<T> clazz;
  private Set<String> attributes;
  private SecondaryIndexType secondaryIndexType;
  private List<String> fieldIds;

  public AbstractSecondaryIndexConfiguration(
      final Class<T> clazz,
      final String attribute,
      final SecondaryIndexType secondaryIndexType) {
    this(clazz, Sets.newHashSet(attribute), secondaryIndexType);
  }

  public AbstractSecondaryIndexConfiguration(
      final Class<T> clazz,
      final Set<String> attributes,
      final SecondaryIndexType secondaryIndexType) {
    this(clazz, attributes, secondaryIndexType, Collections.<String>emptyList());
  }

  public AbstractSecondaryIndexConfiguration(
      final Class<T> clazz,
      final String attribute,
      final SecondaryIndexType secondaryIndexType,
      final List<String> fieldIds) {
    this(clazz, Sets.newHashSet(attribute), secondaryIndexType, fieldIds);
  }

  public AbstractSecondaryIndexConfiguration(
      final Class<T> clazz,
      final Set<String> attributes,
      final SecondaryIndexType secondaryIndexType,
      final List<String> fieldIds) {
    super();
    this.clazz = clazz;
    this.attributes = attributes;
    this.secondaryIndexType = secondaryIndexType;
    this.fieldIds = fieldIds;
    if (secondaryIndexType.equals(SecondaryIndexType.PARTIAL)
        && (fieldIds == null || fieldIds.isEmpty())) {
      throw new RuntimeException(
          "A list of fieldIds must be provided when using a PARTIAL index type");
    }
  }

  public Set<String> getAttributes() {
    return attributes;
  }

  public List<String> getFieldIds() {
    return fieldIds;
  }

  @Override
  public void updateType(final SimpleFeatureType type) {
    for (final String attribute : attributes) {
      final AttributeDescriptor desc = type.getDescriptor(attribute);
      if (desc != null) {
        final Class<?> attributeType = desc.getType().getBinding();
        if (clazz.isAssignableFrom(attributeType)) {
          desc.getUserData().put(getIndexKey(), secondaryIndexType.getValue());
          if (secondaryIndexType.equals(SecondaryIndexType.PARTIAL)) {
            desc.getUserData().put(secondaryIndexType.getValue(), Joiner.on(",").join(fieldIds));
          }
        } else {
          LOGGER.error(
              "Expected type "
                  + clazz.getName()
                  + " for attribute '"
                  + attribute
                  + "' but found "
                  + attributeType.getName());
        }
      } else {
        LOGGER.error(
            "SimpleFeatureType does not contain an AttributeDescriptor that matches '"
                + attribute
                + "'");
      }
    }
  }

  @Override
  public void configureFromType(final SimpleFeatureType type) {
    for (final AttributeDescriptor desc : type.getAttributeDescriptors()) {
      if ((desc.getUserData().get(getIndexKey()) != null)
          && (desc.getUserData().get(getIndexKey()).equals(secondaryIndexType.getValue()))) {
        attributes.add(desc.getLocalName());
      }
      if (desc.getUserData().containsKey(SecondaryIndexType.PARTIAL.getValue())) {
        String joined = (String) desc.getUserData().get(SecondaryIndexType.PARTIAL.getValue());
        final Iterable<String> fields = Splitter.on(",").split(joined);
        for (String field : fields) {
          fieldIds.add(field);
        }
      }
    }
  }

  @Override
  public byte[] toBinary() {
    byte[] clazzBytes = StringUtils.stringToBinary(clazz.getName());
    byte[] attributesBytes = StringUtils.stringsToBinary(attributes.toArray(new String[0]));
    byte secondaryIndexTypeByte = (byte) secondaryIndexType.ordinal();
    byte[] fieldIdsBytes = StringUtils.stringsToBinary(fieldIds.toArray(new String[0]));
    ByteBuffer buf =
        ByteBuffer.allocate(
            1
                + VarintUtils.unsignedIntByteLength(clazzBytes.length)
                + clazzBytes.length
                + VarintUtils.unsignedIntByteLength(attributesBytes.length)
                + attributesBytes.length
                + VarintUtils.unsignedIntByteLength(fieldIdsBytes.length)
                + fieldIdsBytes.length);
    VarintUtils.writeUnsignedInt(clazzBytes.length, buf);
    buf.put(clazzBytes);
    buf.put(secondaryIndexTypeByte);
    VarintUtils.writeUnsignedInt(attributesBytes.length, buf);
    buf.put(attributesBytes);
    VarintUtils.writeUnsignedInt(fieldIdsBytes.length, buf);
    buf.put(fieldIdsBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    byte[] clazzBytes = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(clazzBytes);
    byte secondaryIndexTypeByte = buf.get();
    byte[] attributesBytes = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(attributesBytes);
    byte[] fieldIdsBytes = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(fieldIdsBytes);
    try {
      clazz = (Class<T>) Class.forName(StringUtils.stringFromBinary(clazzBytes));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Class not found when deserializing", e);
    }
    secondaryIndexType = SecondaryIndexType.values()[secondaryIndexTypeByte];
    attributes = new HashSet<>(Arrays.asList(StringUtils.stringsFromBinary(attributesBytes)));
    fieldIds = Arrays.asList(StringUtils.stringsFromBinary(fieldIdsBytes));
  }
}
