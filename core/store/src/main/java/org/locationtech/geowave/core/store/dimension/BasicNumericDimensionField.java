/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.dimension;

import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Sets;

public class BasicNumericDimensionField<T extends Number> extends AbstractNumericDimensionField<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicNumericDimensionField.class);
  private String fieldName;
  private Class<T> myClass;

  public BasicNumericDimensionField() {
    super();
  }

  public BasicNumericDimensionField(final String fieldName, final Class<T> myClass) {
    this(fieldName, myClass, null);
  }

  public BasicNumericDimensionField(
      final String fieldName,
      final Class<T> myClass,
      final Range<Double> range) {
    super(
        range == null ? null
            : new BasicDimensionDefinition(range.getMinimum(), range.getMaximum()));
    this.fieldName = fieldName;
    this.myClass = myClass;
  }

  @Override
  public NumericData getNumericData(final T dataElement) {
    return new NumericValue(dataElement.doubleValue());
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public FieldWriter<T> getWriter() {
    return FieldUtils.getDefaultWriterForClass(myClass);
  }

  @Override
  public FieldReader<T> getReader() {
    return FieldUtils.getDefaultReaderForClass(myClass);
  }

  @Override
  public Class<T> getFieldClass() {
    return myClass;
  }

  @Override
  public byte[] toBinary() {
    final byte[] bytes;
    if (baseDefinition != null) {
      bytes = baseDefinition.toBinary();
    } else {
      bytes = new byte[0];
    }
    final byte[] strBytes = StringUtils.stringToBinary(fieldName);
    final byte[] classBytes = StringUtils.stringToBinary(myClass.getName());
    final ByteBuffer buf =
        ByteBuffer.allocate(
            bytes.length
                + VarintUtils.unsignedIntByteLength(strBytes.length)
                + strBytes.length
                + VarintUtils.unsignedIntByteLength(classBytes.length)
                + classBytes.length);
    VarintUtils.writeUnsignedInt(strBytes.length, buf);
    buf.put(strBytes);
    VarintUtils.writeUnsignedInt(classBytes.length, buf);
    buf.put(classBytes);
    buf.put(bytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int fieldNameLength = VarintUtils.readUnsignedInt(buf);
    final byte[] strBytes = ByteArrayUtils.safeRead(buf, fieldNameLength);
    fieldName = StringUtils.stringFromBinary(strBytes);
    final int classNameLength = VarintUtils.readUnsignedInt(buf);
    final byte[] classBytes = ByteArrayUtils.safeRead(buf, classNameLength);
    final String className = StringUtils.stringFromBinary(classBytes);
    try {
      myClass = (Class<T>) Class.forName(className);
    } catch (final ClassNotFoundException e) {
      LOGGER.warn("Unable to read class", e);
    }
    final int restLength =
        bytes.length
            - VarintUtils.unsignedIntByteLength(fieldNameLength)
            - fieldNameLength
            - VarintUtils.unsignedIntByteLength(classNameLength)
            - classNameLength;
    if (restLength > 0) {
      final byte[] rest = ByteArrayUtils.safeRead(buf, restLength);
      baseDefinition = new BasicDimensionDefinition();
      baseDefinition.fromBinary(rest);
    } else {
      baseDefinition = null;
    }
  }

  @Override
  public Set<IndexDimensionHint> getDimensionHints() {
    return Sets.newHashSet();
  }

}
