/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.adapter.vector.plugin.visibility;

import java.io.ObjectStreamException;
import java.nio.ByteBuffer;

import org.locationtech.geowave.core.geotime.util.SimpleFeatureUserDataConfiguration;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.data.visibility.VisibilityManagement;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *
 * Describes which attribute in a feature contains the visibility constraints,
 * interpreted by a {@link ColumnVisibilityManagementSpi}
 *
 */
public class VisibilityConfiguration implements
		SimpleFeatureUserDataConfiguration
{

	private static final long serialVersionUID = -664252700036603897L;
	private String attributeName = "GEOWAVE_VISIBILITY";
	private String managerClassName = JsonDefinitionColumnVisibilityManagement.class.getName();
	private transient VisibilityManagement<SimpleFeature> manager = new JsonDefinitionColumnVisibilityManagement<>();

	public VisibilityConfiguration() {

	}

	public VisibilityConfiguration(
			final SimpleFeatureType type ) {
		configureFromType(type);
	}

	/**
	 * Check to see if provided type needs to be updated with the provided
	 * manager. If no visibilityManager has been set and a valid manager is
	 * passed in, then set the SimpleFeatureType to reflect the new manager
	 *
	 * @param type
	 *            - feature type object to be checked and updated
	 * @param manager
	 *            - VisibilityManagement object to be used for this type
	 */

	public void updateWithDefaultIfNeeded(
			final SimpleFeatureType type,
			final VisibilityManagement<SimpleFeature> manager ) {
		if ((!configureManager(type)) && (manager != null)) {
			this.manager = manager;
			managerClassName = manager.getClass().getName();
			updateType(type);
		}
	}

	public String getAttributeName() {
		return attributeName;
	}

	/**
	 * USED FOR TESTING
	 *
	 * @param attributeName
	 */
	public void setAttributeName(
			final String attributeName ) {
		if (attributeName != null) {
			this.attributeName = attributeName;
		}
	}

	public String getManagerClassName() {
		return managerClassName;
	}

	public void setManagerClassName(
			final String managerClassName ) {
		this.managerClassName = managerClassName;
	}

	@JsonIgnore
	public VisibilityManagement<SimpleFeature> getManager() {
		return manager;
	}

	/**
	 * {@inheritDoc} Method that updates visibility for the passed in
	 * SimpleFeatureType.
	 *
	 * @param persistType
	 *            - type object to be updated
	 */
	@Override
	public void updateType(
			final SimpleFeatureType persistType ) {
		// First, remove the visibility UserData from all attributes
		for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
			attrDesc.getUserData().remove(
					"visibility");
		}

		final AttributeDescriptor attrDesc = persistType.getDescriptor(attributeName);
		if (attrDesc != null) {
			attrDesc.getUserData().put(
					"visibility",
					Boolean.TRUE);
		}

		persistType.getUserData().put(
				"visibilityManagerClass",
				managerClassName);
	}

	/**
	 * {@inheritDoc} Configure this VisibilityConfiguration object based on the
	 * passed in SimpleFeatureType. This includes setting the 'attributeName' to
	 * the attribute that has a key of 'visiblity'.
	 *
	 * @param persistType
	 *            - object used to configure this VisibilityConfiguration object
	 *
	 */
	@Override
	public void configureFromType(
			final SimpleFeatureType persistType ) {
		// Search the list of attributes for one that has user data
		// with a key of 'visibility' and that the value of
		// it is Boolean.TRUE. If found, set this object's attributeName to
		// the found attribute.

		for (final AttributeDescriptor attrDesc : persistType.getAttributeDescriptors()) {
			if (attrDesc.getUserData().containsKey(
					"visibility") && Boolean.TRUE.equals(attrDesc.getUserData().get(
					"visibility"))) {
				attributeName = attrDesc.getLocalName();
			}
		}
		configureManager(persistType);
	}

	@SuppressWarnings("unchecked")
	private boolean configureManager(
			final SimpleFeatureType persistType ) {
		final Object visMgr = persistType.getUserData().get(
				"visibilityManagerClass");
		if (visMgr == null) {
			// If no visibility manager is present, then can't configure
			return false;
		}

		// If the manager class name
		if ((managerClassName == null) || (!visMgr.toString().equals(
				managerClassName))) {
			try {
				managerClassName = visMgr.toString();
				manager = (VisibilityManagement<SimpleFeature>) Class.forName(
						visMgr.toString()).newInstance();
			}
			catch (final Exception ex) {
				VisibilityManagementHelper.LOGGER.warn(
						"Cannot load visibility management class " + visMgr.toString(),
						ex);
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public Object readResolve()
			throws ObjectStreamException {
		if ((managerClassName != null) && !(manager instanceof JsonDefinitionColumnVisibilityManagement)) {
			try {
				manager = (VisibilityManagement<SimpleFeature>) Class.forName(
						managerClassName).newInstance();
			}
			catch (final Exception ex) {
				VisibilityManagementHelper.LOGGER.warn(
						"Cannot load visibility management class " + managerClassName,
						ex);
			}
		}
		return this;
	}

	@Override
	public byte[] toBinary() {
		byte[] managerClassBytes;
		if (managerClassName != null) {
			managerClassBytes = StringUtils.stringToBinary(managerClassName);
		}
		else {
			managerClassBytes = new byte[0];
		}
		byte[] attributeBytes;
		if (attributeName != null) {
			attributeBytes = StringUtils.stringToBinary(attributeName);
		}
		else {
			attributeBytes = new byte[0];
		}
		final ByteBuffer buf = ByteBuffer.allocate(attributeBytes.length + managerClassBytes.length + 8);
		buf.putInt(attributeBytes.length);
		buf.put(attributeBytes);
		buf.putInt(managerClassBytes.length);
		buf.put(managerClassBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		byte[] attributeBytes;
		final byte[] managerClassBytes;
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		attributeBytes = new byte[buf.getInt()];
		if (attributeBytes.length > 0) {
			buf.get(attributeBytes);
			attributeName = StringUtils.stringFromBinary(attributeBytes);
		}
		else {
			attributeName = null;
		}
		managerClassBytes = new byte[buf.getInt()];
		if (managerClassBytes.length > 0) {
			buf.get(managerClassBytes);
			managerClassName = StringUtils.stringFromBinary(managerClassBytes);
		}
		else {
			managerClassName = null;
		}
		if ((managerClassName != null)
				&& !(managerClassName.equals(JsonDefinitionColumnVisibilityManagement.class.getName()))) {
			try {
				manager = (VisibilityManagement<SimpleFeature>) Class.forName(
						managerClassName).newInstance();
			}
			catch (final Exception ex) {
				VisibilityManagementHelper.LOGGER.warn(
						"Cannot load visibility management class " + managerClassName,
						ex);
			}
		}
	}
}
