package mil.nga.giat.geowave.examples.ingest;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.gt.GeoWaveDataStore;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;


public class SimpleIngestProducerConsumer extends SimpleIngest {

	private static Logger log = Logger.getLogger(SimpleIngestProducerConsumer.class);
	private FeatureCollection _features = new FeatureCollection();

	
	
	/***
	 * Here we will change the ingest mechanism to use a producer/consumer pattern
	 */
	@Override
	protected void GenerateGrid(BasicAccumuloOperations bao){
	
		//create our datastore object
		final DataStore geowaveDataStore = getGeowaveDataStore(bao);
		
		//In order to store data we need to determine the type of data store
		SimpleFeatureType point = createPointFeatureType();
		
		//This a factory class that builds simple feature objects based on the type passed
		SimpleFeatureBuilder pointBuilder = new SimpleFeatureBuilder(point);
		
		//This is an adapter, that is needed to describe how to persist the data type passed
		final FeatureDataAdapter adapter = createDataAdapter(point);
		
		//This describes how to index the data
		final Index index = createSpatialIndex();
		
		//features require a featureID - this should be unqiue as it's a foreign key on the feature 
		//(i.e. sending in a new feature with the same feature id will overwrite the existing feature)
		int featureId = 0;
		
		final Thread ingestThread = new Thread(
				new Runnable() {
					@Override
					public void run() {
						geowaveDataStore.ingest(adapter, index, _features);
					}
				}, "Ingestion Thread"
			);
		

		ingestThread.run();
		
		//build a grid of points across the globe at each whole lattitude/longitude intersection
		for (int longitude = -180; longitude <= 180; longitude++){
			for (int latitude = -90; latitude <= 90; latitude++){
				pointBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
				pointBuilder.set("TimeStamp", new Date());
				pointBuilder.set("Latitude", latitude);
				pointBuilder.set("Longitude", longitude);
				//Note since trajectoryID and comment are marked as nillable we don't need to set them (they default ot null).
				
				SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				featureId++;
				//geowaveDataStore.ingest(adapter, index, sft);
				_features.add(sft);
			}
		}
		_features.IngestCompleted = true;
		try {
			ingestThread.join();
		} catch (InterruptedException e) {
			log.error("Error joining ingest thread", e);
		}
	}
	
	protected class FeatureCollection implements Iterator<SimpleFeature>{
		private BlockingQueue<SimpleFeature> queue = new LinkedBlockingQueue<SimpleFeature>(10000);
		public boolean IngestCompleted = false;

		
		public void add(SimpleFeature sft){
			queue.add(sft);
		}
		
		@Override
		public boolean hasNext() {
			return !(IngestCompleted || queue.isEmpty());
		}

		@Override
		public SimpleFeature next() {
			try {
				return queue.take();
			} catch (InterruptedException e) {
				log.error("Error getting next item from queue", e);
			}
			return null;
		}

		@Override
		public void remove() {
			log.error("Remove called, method not implemented");
			
		}
		
		
		
		
	}
	
	
}
