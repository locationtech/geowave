package mil.nga.giat.geowave.adapter.vector.ingest;

import java.nio.ByteBuffer;

import com.beust.jcommander.Parameter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;

public class GeometrySimpOptionProvider implements
		Persistable
{
	@Parameter(names = "--maxVertices", description = "Maximum number of vertices to allow for the feature. Features with over this vertice count will be discarded.")
	private int maxVertices = Integer.MAX_VALUE;

	@Parameter(names = "--minSimpVertices", description = "Minimum vertex count to qualify for geometry simplification.")
	private int simpVertMin = Integer.MAX_VALUE;

	@Parameter(names = "--tolerance", description = "Maximum error tolerance in geometry simplification. Should range from 0.0 to 1.0 (i.e. .1 = 10%)")
	private double tolerance = 0.02;

	public Geometry simplifyGeometry(
			Geometry geom ) {
		if (geom.getCoordinates().length > this.simpVertMin) {
			return DouglasPeuckerSimplifier.simplify(
					geom,
					this.tolerance);
		}
		return geom;
	}

	public boolean filterGeometry(
			Geometry geom ) {
		return (geom.getCoordinates().length < this.maxVertices && !geom.isEmpty() && geom.isValid());
	}

	@Override
	public byte[] toBinary() {
		final byte[] backingBuffer = new byte[Integer.BYTES * 2 + Double.BYTES];
		ByteBuffer buf = ByteBuffer.wrap(backingBuffer);
		buf.putInt(
				maxVertices).putInt(
				simpVertMin).putDouble(
				tolerance);
		return backingBuffer;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		maxVertices = buf.getInt();
		simpVertMin = buf.getInt();
		tolerance = buf.getDouble();
	}

	public int getMaxVertices() {
		return maxVertices;
	}

	public int getSimpLimit() {
		return simpVertMin;
	}

	public double getTolerance() {
		return tolerance;
	}

}
