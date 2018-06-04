package mil.nga.giat.geowave.format.gpx;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class GeometrySimpOptionProvider implements
Persistable
{
	@Parameter(names = "--maxVertices", description = "Maximum number of vertices to allow for the feature. Features with over this vertice count will be discarded.")
	private int maxVertices = 500;
	
	@Parameter(names = "--minSimpVertices", description = "Minimum vertex count to qualify for geometry simplification.")
	private int simpLimit = 20;
	
	@Parameter(names = "--tolerance", description = "Maximum error tolerance in geometry simplification. Should range from 0.0 to 1.0 (i.e. .1 = 10%)")
	private double tolerance = 0.02;
	
	@Parameter(names = "--maxLength", description = "Maximum line length for gpx track in degrees.")
	private double lineLength = 10.0;

	@Override
	public byte[] toBinary() {
		return new byte[] {
			(byte)maxVertices,
			(byte)simpLimit,
			(byte)tolerance,
			(byte)lineLength
		};
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		int offset = 0;
		maxVertices = (int)bytes[offset];
		offset += 4;
		simpLimit = (int)bytes[offset];
		offset += 4;
		tolerance = (double)bytes[offset];
		offset += 4;
		lineLength = (double)bytes[offset];
	}

	public int getMaxVertices() {
		return maxVertices;
	}

	public int getSimpLimit() {
		return simpLimit;
	}

	public double getTolerance() {
		return tolerance;
	}
	
	public double getMaxLength() {
		return lineLength;
	}

}
