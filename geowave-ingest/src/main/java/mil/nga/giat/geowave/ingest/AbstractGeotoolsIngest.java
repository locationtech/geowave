package mil.nga.giat.geowave.ingest;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.field.GlobalVisibilityHandler;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

abstract public class AbstractGeotoolsIngest
{
	private final static Logger LOGGER = Logger.getLogger(AbstractGeotoolsIngest.class);
	protected final mil.nga.giat.geowave.store.DataStore geowaveDataStore;
	protected final IndexType indexType;
	protected final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;

	public AbstractGeotoolsIngest(
			AccumuloOperations operations,
			IndexType indexType,
			String visibility ) {
		geowaveDataStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						operations),
				new AccumuloAdapterStore(
						operations),
				operations);
		this.indexType = indexType;
		if (visibility != null && !visibility.isEmpty()) {
			visibilityHandler = new GlobalVisibilityHandler<SimpleFeature, Object>(
					visibility);
		}
		else {
			visibilityHandler = null;
		}
	}

	protected boolean ingestDataStore(
			DataStore dataStore )
			throws IOException {
		List<Name> names = dataStore.getNames();
		boolean success = true;
		for (Name name : names) {
			SimpleFeatureSource source = dataStore.getFeatureSource(name);
			SimpleFeatureCollection featureCollection = source.getFeatures();
			SimpleFeatureCollectionIterable iter = new SimpleFeatureCollectionIterable(featureCollection);
			try {
				geowaveDataStore.ingest(
						new FeatureDataAdapter(
								featureCollection.getSchema()),
						indexType.createDefaultIndex(),
						iter.iterator());

			}
			catch (Exception e) {
				LOGGER.error(
						"Unable to ingest data source for feature name '" + name + "'",
						e);
			} finally {
				iter.close();
				
			}
		}
		return success;
	}
}
