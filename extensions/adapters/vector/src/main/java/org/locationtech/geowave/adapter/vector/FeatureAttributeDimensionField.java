package org.locationtech.geowave.adapter.vector;

import java.nio.ByteBuffer;
import org.apache.commons.lang3.Range;
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

  public FeatureAttributeDimensionField(
      final AttributeDescriptor attributeDescriptor,
      final Range<Double> range) {
    super(new BasicDimensionDefinition(range.getMinimum(), range.getMaximum()));
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
    final byte[] bytes = baseDefinition.toBinary();
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
    final byte[] strBytes = new byte[attrNameLength];
    buf.get(strBytes);
    attributeName = StringUtils.stringFromBinary(strBytes);
    final byte[] rest =
        new byte[bytes.length - VarintUtils.unsignedIntByteLength(attrNameLength) - attrNameLength];
    buf.get(rest);
    baseDefinition = new BasicDimensionDefinition();
    baseDefinition.fromBinary(rest);
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
