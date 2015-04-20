package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;

import org.apache.log4j.Logger;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * This class persists information associated with the requested map -
 * Coordinate Reference System and bounding box. It is important that the
 * distributed rendering is performed in the requested CRS, or there will be
 * artifacts.
 * 
 */
public class ServerMapArea implements
		Persistable
{
	private final static Logger LOGGER = Logger.getLogger(ServerMapArea.class);

	private ReferencedEnvelope env;

	protected ServerMapArea() {}

	public ServerMapArea(
			final ReferencedEnvelope env ) {
		this.env = env;
	}

	public CoordinateReferenceSystem getCRS() {
		return env.getCoordinateReferenceSystem();
	}

	public ReferencedEnvelope getBounds() {
		return env;
	}

	@Override
	public byte[] toBinary() {
		// we are assuming the data is transmitted in the default reference
		// system
		final double minX = env.getMinX();
		final double minY = env.getMinY();
		final double maxX = env.getMaxX();
		final double maxY = env.getMaxY();
		final String wkt = env.getCoordinateReferenceSystem().toWKT();
		final byte[] wktBinary = StringUtils.stringToBinary(wkt);
		final ByteBuffer buf = ByteBuffer.allocate(32 + wktBinary.length);
		buf.putDouble(minX);
		buf.putDouble(minY);
		buf.putDouble(maxX);
		buf.putDouble(maxY);
		buf.put(wktBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);

		final double minX = buf.getDouble();
		final double minY = buf.getDouble();
		final double maxX = buf.getDouble();
		final double maxY = buf.getDouble();
		final byte[] wktBinary = new byte[bytes.length - 32];
		buf.get(wktBinary);
		final String wkt = StringUtils.stringFromBinary(wktBinary);
		try {
			env = new ReferencedEnvelope(
					minX,
					maxX,
					minY,
					maxY,
					CRS.parseWKT(wkt));
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to parse coordinate reference system",
					e);
			env = new ReferencedEnvelope(
					minX,
					maxX,
					minY,
					maxY,
					GeoWaveGTDataStore.DEFAULT_CRS);
		}

	}
}
