/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.vector.query;

public class CqlQueryFilterIteratorTest
{
	// TODO figure out if we need this test, I don't think it was really testing
	// what it seems to intend to test though because MockAccumulo is not going
	// to use the VFSClassloader, we can test URLstreamhandlerfactory without a
	// dependency on cql or a dependency on accumulo

	// private DataStore createDataStore()
	// throws IOException {
	// final Map<String, Serializable> params = new HashMap<String,
	// Serializable>();
	// params.put(
	// "gwNamespace",
	// "test_" + getClass().getName());
	// return new GeoWaveGTDataStoreFactory(
	// new MemoryStoreFactoryFamily()).createNewDataStore(params);
	// }
	//
	// @Test
	// public void test()
	// throws SchemaException,
	// IOException,
	// ParseException {
	// final DataStore dataStore = createDataStore();
	//
	// final SimpleFeatureType type = DataUtilities.createType(
	// "CqlQueryFilterIteratorTest",
	// "geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");
	//
	// dataStore.createSchema(type);
	//
	// final Transaction transaction1 = new DefaultTransaction();
	//
	// final FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
	// dataStore.getFeatureWriter(
	// "CqlQueryFilterIteratorTest",
	// transaction1);
	// final SimpleFeature newFeature = writer.next();
	// newFeature.setAttribute(
	// "pop",
	// Long.valueOf(100));
	// newFeature.setAttribute(
	// "pid",
	// "a89dhd-123-dxc");
	// newFeature.setAttribute(
	// "geometry",
	// new WKTReader().read("LINESTRING (30 10, 10 30, 40 40)"));
	// writer.write();
	// writer.close();
	//
	// transaction1.commit();
	//
	// final FilterFactoryImpl factory = new FilterFactoryImpl();
	// final Expression exp1 = factory.property("pid");
	// final Expression exp2 = factory.literal("a89dhd-123-dxc");
	// final Filter f = factory.equal(
	// exp1,
	// exp2,
	// false);
	//
	// final MockInstance mockDataInstance = new MockInstance(
	// "CqlQueryFilterIteratorTest");
	// final Connector mockDataConnector = mockDataInstance.getConnector(
	// "root",
	// new PasswordToken(
	// new byte[0]));
	// final BasicAccumuloOperations dataOps = new BasicAccumuloOperations(
	// mockDataConnector);
	//
	// final AccumuloIndexStore indexStore = new AccumuloIndexStore(
	// dataOps);
	//
	// final String tableName = IndexType.SPATIAL_VECTOR.getDefaultId();
	// final ScannerBase scanner = dataOps.createScanner(tableName);
	//
	// final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
	// dataOps);
	//
	// initScanner(
	// scanner,
	// indexStore.getIndex(new ByteArrayId(
	// IndexType.SPATIAL_VECTOR.getDefaultId())),
	// (DataAdapter<SimpleFeature>) adapterStore.getAdapter(new ByteArrayId(
	// "CqlQueryFilterIteratorTest")),
	// f);
	//
	// final Iterator<Entry<Key, Value>> it = scanner.iterator();
	// assertTrue(it.hasNext());
	// int count = 0;
	// while (it.hasNext()) {
	// it.next();
	// count++;
	// }
	// // line string covers more than one tile
	// assertTrue(count >= 1);
	//
	// }
	//
	// @Test
	// public void testStreamHandlerFactoryConflictResolution() {
	// unsetURLStreamHandlerFactory();
	// URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	// try {
	// Class.forName(CqlQueryFilterIterator.class.getName());
	// }
	// catch (final Exception e) {
	// Assert.fail("Iterator did not handle an alread loaded URLStreamHandler, exception was: "
	// + e.getLocalizedMessage());
	// }
	// catch (final Error e) {
	// Assert.fail("Iterator did not handle an alread loaded URLStreamHandler, error was: "
	// + e.getLocalizedMessage());
	// }
	// Assert.assertEquals(
	// unsetURLStreamHandlerFactory(),
	// FsUrlStreamHandlerFactory.class.getName());
	// URL.setURLStreamHandlerFactory(new UnitTestCustomStreamHandlerFactory());
	// try {
	// final Method m = CqlQueryFilterIterator.class.getDeclaredMethod(
	// "initialize",
	// null);
	// m.setAccessible(true);
	// m.invoke(null);
	// }
	// catch (final NoSuchMethodException e) {
	// Assert.fail("Error changing scope of CqlQueryFilterIterator init() method");
	// }
	// catch (final InvocationTargetException e) {
	// if (e.getTargetException().getMessage().equals(
	// "factory already defined")) {
	// Assert.assertEquals(
	// unsetURLStreamHandlerFactory(),
	// UnitTestCustomStreamHandlerFactory.class.getName());
	// URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	// return;
	// }
	// Assert.fail("Error invoking scope of CqlQueryFilterIterator init() method");
	// }
	// catch (final IllegalAccessException e) {
	// Assert.fail("Error accessing scope of CqlQueryFilterIterator init() method");
	// }
	// Assert.fail("Loading conflicting duplicate StreamHandler factories did not throw an error");
	// }
	//
	// private static String unsetURLStreamHandlerFactory() {
	// try {
	// final Field f = URL.class.getDeclaredField("factory");
	// f.setAccessible(true);
	// final Object curFac = f.get(null);
	// f.set(
	// null,
	// null);
	// URL.setURLStreamHandlerFactory(null);
	// return curFac.getClass().getName();
	// }
	// catch (final Exception e) {
	// return null;
	// }
	// }
	//
	// public class UnitTestCustomStreamHandlerFactory implements
	// java.net.URLStreamHandlerFactory
	// {
	// public UnitTestCustomStreamHandlerFactory() {}
	//
	// @Override
	// public URLStreamHandler createURLStreamHandler(
	// final String protocol ) {
	// if (protocol.equals("http")) {
	// return new sun.net.www.protocol.http.Handler();
	// }
	// else if (protocol.equals("https")) {
	// return new sun.net.www.protocol.https.Handler();
	// }
	// return null;
	// }
	// }
	//
	// private void initScanner(
	// final ScannerBase scanner,
	// final Index index,
	// final DataAdapter<SimpleFeature> dataAdapter,
	// final Filter cqlFilter ) {
	// final IteratorSetting iteratorSettings = new IteratorSetting(
	// CqlQueryFilterIterator.CQL_QUERY_ITERATOR_PRIORITY,
	// CqlQueryFilterIterator.CQL_QUERY_ITERATOR_NAME,
	// CqlQueryFilterIterator.class);
	// iteratorSettings.addOption(
	// CqlQueryFilterIterator.CQL_FILTER,
	// FilterToCQLTool.toCQL(cqlFilter));
	// iteratorSettings.addOption(
	// CqlQueryFilterIterator.DATA_ADAPTER,
	// ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(dataAdapter)));
	// iteratorSettings.addOption(
	// CqlQueryFilterIterator.MODEL,
	// ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));
	//
	// scanner.addScanIterator(iteratorSettings);
	// }

}
