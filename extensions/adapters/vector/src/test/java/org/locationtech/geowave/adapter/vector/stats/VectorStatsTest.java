package org.locationtech.geowave.adapter.vector.stats;

import java.util.ArrayList;
import java.util.Map;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistics;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.Name;

public class VectorStatsTest {
	private DataStore dataStore;
	private Index index;
	
	@Before
	public void createStore() {
		dataStore =  DataStoreFactory.createDataStore(new MemoryRequiredOptions());
		SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		AttributeTypeBuilder attrBldr = new AttributeTypeBuilder();
		SimpleFeatureTypeBuilder typeBuilder2 = new SimpleFeatureTypeBuilder();
		AttributeTypeBuilder attrBldr2 = new AttributeTypeBuilder();
		typeBuilder.setName("city_info");
		typeBuilder.add(attrBldr.binding(Point.class).buildDescriptor("city_center"));
		typeBuilder.add(attrBldr.binding(String.class).buildDescriptor("city_name"));
		typeBuilder.add(attrBldr.binding(Integer.class).buildDescriptor("population"));		
		final FeatureDataAdapter adapter = new FeatureDataAdapter(typeBuilder.buildFeatureType());
		typeBuilder2.setName("taxi");
		typeBuilder2.add(attrBldr2.binding(Point.class).buildDescriptor("location"));
		typeBuilder2.add(attrBldr2.binding(String.class).buildDescriptor("driver"));
		typeBuilder2.add(attrBldr2.binding(Double.class).buildDescriptor("fare"));		
		final FeatureDataAdapter adapter2 =new FeatureDataAdapter(typeBuilder2.buildFeatureType());
		index = new SpatialDimensionalityTypeProvider.SpatialIndexBuilder().createIndex();
		
		dataStore.addType(adapter, index);
		dataStore.addType(adapter2, index);

		try(Writer<Object>writer = dataStore.createWriter(adapter.getTypeName())){
			SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(adapter.getFeatureType());
			bldr.set("city_center", new GeometryFactory().createPoint(new Coordinate(33,44)));
			bldr.set("city_name", "New York");
			bldr.set("population", 102343587);
			writer.write(bldr.buildFeature("NYC"));
			bldr.set("city_center", new GeometryFactory().createPoint(new Coordinate(5000,0)));
			bldr.set("city_name", "Istanbul");
			bldr.set("population", 3000000);
			
			writer.write(bldr.buildFeature("IST"));
		}
		try(Writer<Object>writer = dataStore.createWriter(adapter2.getTypeName())){
			SimpleFeatureBuilder bldr2 = new SimpleFeatureBuilder(adapter2.getFeatureType());
			bldr2.set("location", new GeometryFactory().createPoint(new Coordinate(55,2)));
			bldr2.set("driver", "Max");
			bldr2.set("fare", 32);
			writer.write(bldr2.buildFeature("Trip1"));
		}
	}
	
	@Test
	public void test(){
		StatisticsQuery<Envelope> query= 
		VectorStatisticsQueryBuilder.newBuilder().factory().bbox().build();
		Statistics<Envelope>[] stat = dataStore.queryStatistics(query);
		Assert.assertEquals(stat[0].getResult().getMaxX(), 5000,0);
		Assert.assertEquals(stat[1].getResult().getMaxX(), 55,0);
		Assert.assertEquals(stat.length, 2);
	}
}
