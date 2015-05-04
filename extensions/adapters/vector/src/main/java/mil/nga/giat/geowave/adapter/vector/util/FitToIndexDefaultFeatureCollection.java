package mil.nga.giat.geowave.adapter.vector.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.geotools.data.FeatureReader;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.opengis.util.ProgressListener;

public class FitToIndexDefaultFeatureCollection extends
		DefaultFeatureCollection
{
	private final DefaultFeatureCollection collection;
	private final ByteArrayId indexId;
	private final int subStratIdx;

	public FitToIndexDefaultFeatureCollection(
			final DefaultFeatureCollection collection,
			final ByteArrayId indexId ) {
		this.collection = collection;
		this.indexId = indexId;
		this.subStratIdx = -1;
	}

	public FitToIndexDefaultFeatureCollection(
			final DefaultFeatureCollection collection,
			final ByteArrayId indexId,
			final int subStratIdx ) {
		this.collection = collection;
		this.indexId = indexId;
		this.subStratIdx = subStratIdx;
	}

	public DefaultFeatureCollection getCollection() {
		return collection;
	}

	public ByteArrayId getIndexId() {
		return indexId;
	}

	public int getSubStratIdx() {
		return subStratIdx;
	}

	public ReferencedEnvelope getBounds() {
		return collection.getBounds();
	}

	public boolean add(
			SimpleFeature o ) {
		return collection.add(o);
	}

	public void clear() {
		collection.clear();
	}

	public boolean contains(
			Object o ) {
		return collection.contains(o);
	}

	public boolean containsAll(
			Collection<?> collection ) {
		return this.collection.containsAll(collection);
	}

	public SimpleFeatureIterator features() {
		return collection.features();
	}

	public void close(
			FeatureIterator<SimpleFeature> close ) {
		collection.close(close);
	}

	public int getCount()
			throws IOException {
		return collection.getCount();
	}

	public SimpleFeatureCollection collection()
			throws IOException {
		return collection.collection();
	}

	public void accepts(
			FeatureVisitor visitor,
			ProgressListener progress )
			throws IOException {
		collection.accepts(
				visitor,
				progress);
	}

	public boolean equals(
			Object arg0 ) {
		return collection.equals(arg0);
	}

	public Set fids() {
		return collection.fids();
	}

	public String getID() {
		return collection.getID();
	}

	public SimpleFeatureType getSchema() {
		return collection.getSchema();
	}

	public int hashCode() {
		return collection.hashCode();
	}

	public boolean isEmpty() {
		return collection.isEmpty();
	}

	public Iterator<SimpleFeature> iterator() {
		return collection.iterator();
	}

	public boolean remove(
			Object o ) {
		return collection.remove(o);
	}

	public boolean removeAll(
			Collection<?> collection ) {
		return this.collection.removeAll(collection);
	}

	public boolean retainAll(
			Collection<?> collection ) {
		return this.collection.retainAll(collection);
	}

	public int size() {
		return collection.size();
	}

	public Object[] toArray() {
		return collection.toArray();
	}

	public <T> T[] toArray(
			T[] a ) {
		return collection.toArray(a);
	}

	public FeatureReader<SimpleFeatureType, SimpleFeature> reader()
			throws IOException {
		return collection.reader();
	}

	public SimpleFeatureCollection subCollection(
			Filter filter ) {
		return collection.subCollection(filter);
	}

	public SimpleFeatureCollection sort(
			SortBy order ) {
		return collection.sort(order);
	}

	public void purge() {
		collection.purge();
	}

	public String toString() {
		return collection.toString();
	}

	public void validate() {
		collection.validate();
	}
}
