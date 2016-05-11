package mil.nga.giat.geowave.format.geotools.vector;

import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.format.geotools.vector.retyping.date.DateFieldOptionProvider;

public class GeoToolsVectorDataOptions implements
		IngestFormatOptionProvider
{

	@ParametersDelegate
	private CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();

	@ParametersDelegate
	private DateFieldOptionProvider dateFieldOptionProvider = new DateFieldOptionProvider();

	public GeoToolsVectorDataOptions() {}

	public CQLFilterOptionProvider getCqlFilterOptionProvider() {
		return cqlFilterOptionProvider;
	}

	public void setCqlFilterOptionProvider(
			CQLFilterOptionProvider cqlFilterOptionProvider ) {
		this.cqlFilterOptionProvider = cqlFilterOptionProvider;
	}

	public DateFieldOptionProvider getDateFieldOptionProvider() {
		return dateFieldOptionProvider;
	}

	public void setDateFieldOptionProvider(
			DateFieldOptionProvider dateFieldOptionProvider ) {
		this.dateFieldOptionProvider = dateFieldOptionProvider;
	}
}
