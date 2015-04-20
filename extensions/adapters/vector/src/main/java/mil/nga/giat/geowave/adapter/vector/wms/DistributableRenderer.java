package mil.nga.giat.geowave.adapter.vector.wms;

import mil.nga.giat.geowave.adapter.vector.query.row.AbstractRowProvider;
import mil.nga.giat.geowave.adapter.vector.wms.accumulo.RenderedMaster;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.Persistable;

/**
 * This interface is used to perform rendering within tablet servers. The
 * persistable object can be serialized and transported to an Accumulo Iterator
 * and the iterator can render each object to a series of images (one per style
 * in the case of multiple styles and an additional one for labels to adhere to
 * layering rules). The client of this iterator can then composite the images
 * from the tablet server.
 * 
 */
public interface DistributableRenderer extends
		Persistable
{
	public void render(
			Object content )
			throws Exception;

	public boolean isDecimationEnabled();

	public AbstractRowProvider newRowProvider(
			final NumericIndexStrategy indexStrategy );

	public RenderedMaster getResult();
}
