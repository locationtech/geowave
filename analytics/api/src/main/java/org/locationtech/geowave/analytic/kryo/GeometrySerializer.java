package org.locationtech.geowave.analytic.kryo;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class GeometrySerializer extends Serializer<Geometry> {
  static final Logger LOGGER = LoggerFactory.getLogger(GeometrySerializer.class);

  @Override
  public Geometry read(final Kryo arg0, final Input arg1, final Class<Geometry> arg2) {
    final byte[] data = arg1.readBytes(arg1.readInt());
    try {
      return new WKBReader().read(data);
    } catch (final ParseException e) {
      LOGGER.warn("Unable to deserialize geometry", e);
    }
    return null;
  }

  @Override
  public void write(final Kryo arg0, final Output arg1, final Geometry arg2) {
    final byte[] data = new WKBWriter().write(arg2);
    arg1.writeInt(data.length);
    arg1.write(data);
  }
}
