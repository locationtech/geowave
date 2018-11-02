package org.locationtech.geowave.datastore.redis.util;

import java.io.IOException;

import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Varint;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

public class GeoWaveRedisRowCodec extends
		BaseCodec
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveRedisRowCodec.class);
	protected static GeoWaveRedisRowCodec SINGLETON = new GeoWaveRedisRowCodec();
	private final Decoder<Object> decoder = new Decoder<Object>() {
		@Override
		public Object decode(
				final ByteBuf buf,
				final State state )
				throws IOException {
			try (final ByteBufInputStream in = new ByteBufInputStream(
					buf)) {
				final byte[] dataId = new byte[in.readUnsignedByte()];
				final byte[] fieldMask = new byte[in.readUnsignedByte()];
				final byte[] visibility = new byte[in.readUnsignedByte()];
				final byte[] value = new byte[Varint.readUnsignedVarInt(in)];
				final int numDuplicates = in.readUnsignedByte();
				if (in.read(dataId) != dataId.length) {
					LOGGER.warn("unable to read data ID");
				}
				if (in.read(fieldMask) != fieldMask.length) {
					LOGGER.warn("unable to read fieldMask");
				}
				if (in.read(visibility) != visibility.length) {
					LOGGER.warn("unable to read visibility");
				}
				if (in.read(value) != value.length) {
					LOGGER.warn("unable to read value");
				}
				return new GeoWaveRedisPersistedRow(
						(short) numDuplicates,
						dataId,
						new GeoWaveValueImpl(
								fieldMask,
								visibility,
								value));
			}
		}
	};
	private final Encoder encoder = new Encoder() {
		@Override
		public ByteBuf encode(
				final Object in )
				throws IOException {
			if (in instanceof GeoWaveRedisPersistedRow) {
				final GeoWaveRedisPersistedRow row = (GeoWaveRedisPersistedRow) in;
				final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();

				try (final ByteBufOutputStream out = new ByteBufOutputStream(
						buf)) {
					out.writeByte(row.getDataId().length);
					out.writeByte(row.getFieldMask().length);
					out.writeByte(row.getVisibility().length);
					Varint.writeUnsignedVarInt(
							row.getValue().length,
							out);
					out.writeByte(row.getNumDuplicates());
					out.write(row.getDataId());
					out.write(row.getFieldMask());
					out.write(row.getVisibility());
					out.write(row.getValue());
					out.flush();
					return out.buffer();
				}
			}
			throw new IOException(
					"Encoder only supports GeoWaveRedisRow");
		}
	};

	private GeoWaveRedisRowCodec() {}

	@Override
	public Decoder<Object> getValueDecoder() {
		return decoder;
	}

	@Override
	public Encoder getValueEncoder() {
		return encoder;
	}
}
