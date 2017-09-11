package mil.nga.giat.geowave.format.geotools.raster;

import java.io.IOException;
import java.util.Locale;

import javax.imageio.ImageReader;
import javax.imageio.spi.ImageReaderSpi;

public class ProtobufReaderProvider extends
		ImageReaderSpi
{

	@Override
	public boolean canDecodeInput(
			Object source )
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ImageReader createReaderInstance(
			Object extension )
			throws IOException {
		ProtobufReader reader = null;
		try {
			reader = new ProtobufReader(
					this.getClass().newInstance());
		}
		catch (InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return reader;
	}

	@Override
	public String getDescription(
			Locale locale ) {
		// TODO Auto-generated method stub
		return null;
	}

}
