package mil.nga.giat.geowave.format.geotools.raster;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import javax.imageio.ImageReader;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;

public class ProtobufReaderProvider extends
		ImageReaderSpi
{

	@Override
	public boolean canDecodeInput(
			Object source )
			throws IOException {
		// TODO Make this more selective
		return true;
	}

	@Override
	public ImageReader createReaderInstance(
			Object extension )
			throws IOException {
		ProtobufReader reader = new ProtobufReader(
				this);

		return reader;
	}

	@Override
	public String getDescription(
			Locale locale ) {
		return "my description";
	}

	@Override
	public String getVendorName() {
		return "locationtech";
	}

	@Override
	public String[] getFormatNames() {
		return new String[] {
			"pbf",
			"mbtiles",
			"MBTILES",
			"vector-tile",
			"VECTOR-TILE"
		};
	}

	@Override
	public Class[] getInputTypes() {
		return new Class[] {
			ImageInputStream.class,
		};
	}

}
