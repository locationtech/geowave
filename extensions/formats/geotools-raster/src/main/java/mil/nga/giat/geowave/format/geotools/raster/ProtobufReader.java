package mil.nga.giat.geowave.format.geotools.raster;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos;
import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile;
import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile.Feature;
import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile.Layer;
import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile.Value;

public class ProtobufReader extends
		ImageReader
{

	protected ProtobufReader(
			ImageReaderSpi originatingProvider ) {
		super(
				originatingProvider);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getNumImages(
			boolean allowSearch )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getWidth(
			int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHeight(
			int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Iterator<ImageTypeSpecifier> getImageTypes(
			int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IIOMetadata getStreamMetadata()
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IIOMetadata getImageMetadata(
			int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BufferedImage read(
			int imageIndex,
			ImageReadParam param )
			throws IOException {
		Raster readRaster = readRaster(
				imageIndex,
				param);
		/*
		 * BufferedImage img = new BufferedImage(256, 256,
		 * BufferedImage.TYPE_INT_ARGB); Graphics2D g2d =
		 * img.getRaster().setcreateGraphics(); g2d.
		 */

		VectorTileProtos proto;

		return null;
	}

	public Raster readRaster(
			final int imageIndex,
			ImageReadParam param )
			throws IOException {
		ImageInputStream inputStream = (ImageInputStream) getInput();
		// TODO read inputStream to get tile data
		return null;

	}

	public Raster createRaster(
			Tile tile ) {
		WritableRaster raster;

		/*
		 * int[] bandMasks; bandMasks = new int[2]; raster =
		 * Raster.createPackedRaster(DataBuffer.TYPE_INT, 256, 256, bandMasks,
		 * null);
		 */

		raster = Raster.createPackedRaster(
				DataBuffer.TYPE_INT,
				256,
				256,
				2,
				16,
				null);

		int density;
		int count;
		int xValue = 0;
		int yValue = 0;

		// calculate resolution level
		// tile.getLayers(0).getFeatures(0).getGeometryList().get

		for (Layer layer : tile.getLayersList()) {
			for (Feature feature : layer.getFeaturesList()) {
				// TODO calculate and set the grid values
			}
			density = (int) layer.getValues(
					0).getIntValue();
			count = (int) layer.getValues(
					1).getIntValue();

			// Count is band 0, density is band 1
			raster.setSample(
					xValue,
					yValue,
					0,
					count);
			raster.setSample(
					xValue,
					yValue,
					1,
					density);
		}

		return raster;
	}

}
