package mil.nga.giat.geowave.datastore.accumulo.util;

import java.io.Closeable;

import org.apache.accumulo.core.client.ScannerBase;

public class ScannerClosableWrapper implements
		Closeable
{
	private final ScannerBase scanner;

	public ScannerClosableWrapper(
			final ScannerBase scanner ) {
		this.scanner = scanner;
	}

	@Override
	public void close() {
		scanner.close();
	}

}
