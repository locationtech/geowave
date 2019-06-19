/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import java.nio.ByteBuffer;
import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.AbstractNumericDimensionField;
import org.opengis.feature.type.AttributeDescriptor;

public class FeatureAttributeDimensionField extends
    AbstractNumericDimensionField<FeatureAttributeCommonIndexValue> {
  private FieldWriter<?, FeatureAttributeCommonIndexValue> writer;
  private FieldReader<FeatureAttributeCommonIndexValue> reader;
  private String attributeName;

  public FeatureAttributeDimensionField() {
    super();
  }

  public FeatureAttributeDimensionField(final AttributeDescriptor attributeDescriptor) {
    this(attributeDescriptor, null);
    attributeName = attributeDescriptor.getLocalName();
  }

  public FeatureAttributeDimensionField(
      final AttributeDescriptor attributeDescriptor,
      final Range<Double> range) {
    super(
        range == null ? null
            : new BasicDimensionDefinition(range.getMinimum(), range.getMaximum()));
    writer =
        new FeatureAttributeWriterWrapper(
            (FieldWriter) FieldUtils.getDefaultWriterForClass(
                attributeDescriptor.getType().getBinding()));
    reader =
        new FeatureAttributeReaderWrapper(
            (FieldReader) FieldUtils.getDefaultReaderForClass(
                attributeDescriptor.getType().getBinding()));
    attributeName = attributeDescriptor.getLocalName();
  }

  @Override
  public NumericData getNumericData(final FeatureAttributeCommonIndexValue dataElement) {
    return new NumericValue(dataElement.getValue().doubleValue());
  }

  @Override
  public String getFieldName() {
    return attributeName;
  }

  @Override
  public FieldWriter<?, FeatureAttributeCommonIndexValue> getWriter() {
    return writer;
  }

  @Override
  public FieldReader<FeatureAttributeCommonIndexValue> getReader() {
    return reader;
  }

  @Override
  public byte[] toBinary() {
    final byte[] bytes;
    if (baseDefinition != null) {
      bytes = baseDefinition.toBinary();
    } else {
      bytes = new byte[0];
    }
    final byte[] strBytes = StringUtils.stringToBinary(attributeName);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            bytes.length + VarintUtils.unsignedIntByteLength(strBytes.length) + strBytes.length);
    VarintUtils.writeUnsignedInt(strBytes.length, buf);
    buf.put(strBytes);
    buf.put(bytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int attrNameLength = VarintUtils.readUnsignedInt(buf);
    final byte[] strBytes = ByteArrayUtils.safeRead(buf, attrNameLength);
    attributeName = StringUtils.stringFromBinary(strBytes);
    final int restLength =
        bytes.length - VarintUtils.unsignedIntByteLength(attrNameLength) - attrNameLength;
    if (restLength > 0) {
      final byte[] rest = ByteArrayUtils.safeRead(buf, restLength);
      baseDefinition = new BasicDimensionDefinition();
      baseDefinition.fromBinary(rest);
    } else {
      baseDefinition = null;
    }
  }

  private static class FeatureAttributeReaderWrapper implements
      FieldReader<FeatureAttributeCommonIndexValue> {
    private final FieldReader<? extends Number> reader;

    public FeatureAttributeReaderWrapper(final FieldReader<? extends Number> reader) {
      this.reader = reader;
    }

    @Override
    public FeatureAttributeCommonIndexValue readField(final byte[] fieldData) {
      return new FeatureAttributeCommonIndexValue(reader.readField(fieldData), null);
    }
  }
  private static class FeatureAttributeWriterWrapper implements
      FieldWriter<Object, FeatureAttributeCommonIndexValue> {
    private final FieldWriter<?, Number> writer;

    public FeatureAttributeWriterWrapper(final FieldWriter<?, Number> writer) {
      this.writer = writer;
    }

    @Override
    public byte[] writeField(final FeatureAttributeCommonIndexValue fieldValue) {
      return writer.writeField(fieldValue.getValue());
    }
  }
}
