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
package mil.nga.giat.geowave.adapter.vector.util;

import java.io.IOException;
import java.util.Collection;

import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class FeatureTranslatingIterator implements
		CloseableIterator<SimpleFeature>
{
	private final SimpleFeatureTranslator translator;
	private final CloseableIterator<SimpleFeature> iteratorDelegate;

	public FeatureTranslatingIterator(
			final SimpleFeatureType originalType,
			final Collection<String> desiredFields,
			final CloseableIterator<SimpleFeature> originalFeatures ) {
		translator = new SimpleFeatureTranslator(
				originalType,
				desiredFields);
		iteratorDelegate = originalFeatures;
	}

	@Override
	public boolean hasNext() {
		return iteratorDelegate.hasNext();
	}

	@Override
	public SimpleFeature next() {
		return translator.translate(iteratorDelegate.next());
	}

	@Override
	public void remove() {
		iteratorDelegate.remove();
	}

	@Override
	public void close()
			throws IOException {
		iteratorDelegate.close();

	}

	private static class SimpleFeatureTranslator
	{
		private final Collection<String> fields;
		private SimpleFeatureType newType;
		private SimpleFeatureBuilder sfBuilder;

		public SimpleFeatureTranslator(
				final SimpleFeatureType originalType,
				final Collection<String> fields ) {
			this.fields = fields;
			initialize(originalType);
		}

		private void initialize(
				final SimpleFeatureType originalType ) {
			final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
			sftBuilder.setName(originalType.getName());
			for (final AttributeDescriptor ad : originalType.getAttributeDescriptors()) {
				if (fields.contains(ad.getLocalName())) {
					sftBuilder.add(
							ad.getLocalName(),
							ad.getClass());
				}
			}
			newType = sftBuilder.buildFeatureType();
			sfBuilder = new SimpleFeatureBuilder(
					newType);
		}

		public SimpleFeature translate(
				final SimpleFeature original ) {
			for (final String field : fields) {
				final Object value = original.getAttribute(field);
				if (value != null) {
					sfBuilder.set(
							field,
							value);
				}
			}
			return sfBuilder.buildFeature(original.getID());
		}

	}

}
