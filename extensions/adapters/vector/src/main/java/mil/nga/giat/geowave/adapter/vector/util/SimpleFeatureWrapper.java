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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.IllegalAttributeException;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.FeatureId;
import org.opengis.geometry.BoundingBox;

public class SimpleFeatureWrapper implements
		SimpleFeature
{
	private final SimpleFeature simpleFeature;
	private final ByteArrayId insertionId;
	private final int subStratIdx;

	public SimpleFeatureWrapper(
			final SimpleFeature simpleFeature,
			final ByteArrayId insertionId,
			final int subStratIdx ) {
		this.simpleFeature = simpleFeature;
		this.insertionId = insertionId;
		this.subStratIdx = subStratIdx;
	}

	public SimpleFeature getSimpleFeature() {
		return simpleFeature;
	}

	public ByteArrayId getInsertionId() {
		return insertionId;
	}

	public int getSubStratIdx() {
		return subStratIdx;
	}

	public FeatureId getIdentifier() {
		return simpleFeature.getIdentifier();
	}

	public AttributeDescriptor getDescriptor() {
		return simpleFeature.getDescriptor();
	}

	public BoundingBox getBounds() {
		return simpleFeature.getBounds();
	}

	public String getID() {
		return simpleFeature.getID();
	}

	public SimpleFeatureType getType() {
		return simpleFeature.getType();
	}

	public SimpleFeatureType getFeatureType() {
		return simpleFeature.getFeatureType();
	}

	public void setValue(
			Object newValue ) {
		simpleFeature.setValue(newValue);
	}

	public List<Object> getAttributes() {
		return simpleFeature.getAttributes();
	}

	public GeometryAttribute getDefaultGeometryProperty() {
		return simpleFeature.getDefaultGeometryProperty();
	}

	public void setValue(
			Collection<Property> values ) {
		simpleFeature.setValue(values);
	}

	public void setAttributes(
			List<Object> values ) {
		simpleFeature.setAttributes(values);
	}

	public void setDefaultGeometryProperty(
			GeometryAttribute geometryAttribute ) {
		simpleFeature.setDefaultGeometryProperty(geometryAttribute);
	}

	public Collection<? extends Property> getValue() {
		return simpleFeature.getValue();
	}

	public Collection<Property> getProperties(
			Name name ) {
		return simpleFeature.getProperties(name);
	}

	public void setAttributes(
			Object[] values ) {
		simpleFeature.setAttributes(values);
	}

	public Name getName() {
		return simpleFeature.getName();
	}

	public Property getProperty(
			Name name ) {
		return simpleFeature.getProperty(name);
	}

	public Object getAttribute(
			String name ) {
		return simpleFeature.getAttribute(name);
	}

	public boolean isNillable() {
		return simpleFeature.isNillable();
	}

	public Map<Object, Object> getUserData() {
		return simpleFeature.getUserData();
	}

	public void setAttribute(
			String name,
			Object value ) {
		simpleFeature.setAttribute(
				name,
				value);
	}

	public Collection<Property> getProperties(
			String name ) {
		return simpleFeature.getProperties(name);
	}

	public Object getAttribute(
			Name name ) {
		return simpleFeature.getAttribute(name);
	}

	public void setAttribute(
			Name name,
			Object value ) {
		simpleFeature.setAttribute(
				name,
				value);
	}

	public Collection<Property> getProperties() {
		return simpleFeature.getProperties();
	}

	public Property getProperty(
			String name ) {
		return simpleFeature.getProperty(name);
	}

	public Object getAttribute(
			int index )
			throws IndexOutOfBoundsException {
		return simpleFeature.getAttribute(index);
	}

	public void setAttribute(
			int index,
			Object value )
			throws IndexOutOfBoundsException {
		simpleFeature.setAttribute(
				index,
				value);
	}

	public void validate()
			throws IllegalAttributeException {
		simpleFeature.validate();
	}

	public int getAttributeCount() {
		return simpleFeature.getAttributeCount();
	}

	public Object getDefaultGeometry() {
		return simpleFeature.getDefaultGeometry();
	}

	public void setDefaultGeometry(
			Object geometry ) {
		simpleFeature.setDefaultGeometry(geometry);
	}
}
