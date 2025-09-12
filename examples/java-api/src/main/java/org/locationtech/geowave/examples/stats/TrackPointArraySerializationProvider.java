package org.locationtech.geowave.examples.stats;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.examples.stats.TrackDataModel.TrackPoint;

/**
 * Field serialization provider for TrackPoint arrays. This enables GeoWave to properly serialize
 * and deserialize TrackPoint[] arrays when used in BasicDataTypeAdapter.
 * 
 * This provider will be automatically discovered by GeoWave's SPI mechanism when registered in
 * META-INF/services.
 */
public class TrackPointArraySerializationProvider implements
    FieldSerializationProviderSpi<TrackPoint[]> {

  @Override
  public FieldReader<TrackPoint[]> getFieldReader() {
    return new TrackPointArrayReader();
  }

  @Override
  public FieldWriter<TrackPoint[]> getFieldWriter() {
    return new TrackPointArrayWriter();
  }

  /**
   * Reader for TrackPoint arrays from binary data.
   */
  protected static class TrackPointArrayReader implements FieldReader<TrackPoint[]> {
    @Override
    public TrackPoint[] readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }

      final ByteBuffer buffer = ByteBuffer.wrap(fieldData);
      final int count = VarintUtils.readUnsignedInt(buffer);
      ByteArrayUtils.verifyBufferSize(buffer, count);

      final TrackPoint[] result = new TrackPoint[count];

      for (int i = 0; i < count; i++) {
        // Check if this entry is null
        if (buffer.get() > 0) {
          // Read TrackPoint data
          final double latitude = buffer.getDouble();
          final double longitude = buffer.getDouble();
          final long timestamp = VarintUtils.readSignedLong(buffer);
          final double speed = buffer.getDouble();
          final double acceleration = buffer.getDouble();
          final double heading = buffer.getDouble();
          final double twister = buffer.getDouble();

          result[i] =
              new TrackPoint(latitude, longitude, timestamp, speed, acceleration, heading, twister);
        } else {
          result[i] = null;
        }
      }

      return result;
    }
  }

  /**
   * Writer for TrackPoint arrays to binary data.
   */
  protected static class TrackPointArrayWriter implements FieldWriter<TrackPoint[]> {
    @Override
    public byte[] writeField(final TrackPoint[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }

      // Calculate total buffer size
      int totalBytes = VarintUtils.unsignedIntByteLength(fieldValue.length);

      for (final TrackPoint point : fieldValue) {
        totalBytes++; // null indicator byte
        if (point != null) {
          totalBytes += (6 * Double.BYTES); // 6 double fields
          totalBytes += VarintUtils.signedLongByteLength(point.timestamp); // variable-length
                                                                           // timestamp
        }
      }

      final ByteBuffer buffer = ByteBuffer.allocate(totalBytes);

      // Write array length
      VarintUtils.writeUnsignedInt(fieldValue.length, buffer);

      // Write each TrackPoint
      for (final TrackPoint point : fieldValue) {
        if (point == null) {
          buffer.put((byte) 0x0); // null indicator
        } else {
          buffer.put((byte) 0x1); // non-null indicator

          // Write TrackPoint fields in consistent order
          buffer.putDouble(point.latitude);
          buffer.putDouble(point.longitude);
          VarintUtils.writeSignedLong(point.timestamp, buffer);
          buffer.putDouble(point.speed);
          buffer.putDouble(point.acceleration);
          buffer.putDouble(point.heading);
          buffer.putDouble(point.twister);
        }
      }

      return buffer.array();
    }
  }
}
