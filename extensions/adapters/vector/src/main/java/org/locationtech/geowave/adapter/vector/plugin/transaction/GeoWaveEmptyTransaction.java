/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import org.geotools.data.Transaction;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.factory.Hints;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * Commit changes immediately
 */
public class GeoWaveEmptyTransaction extends AbstractTransactionManagement implements
    GeoWaveTransaction {

  /** Create an empty Diff */
  public GeoWaveEmptyTransaction(final GeoWaveDataStoreComponents components) {
    super(components);
  }

  /** Return true if transaction is empty */
  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public void flush() throws IOException {}

  /**
   * Record a modification to the indicated fid
   *
   * @param fid
   * @param original the original feature(prior state)
   * @param updated the update feature replacement feature; null to indicate remove
   */
  @Override
  public void modify(final String fid, final SimpleFeature original, final SimpleFeature updated)
      throws IOException {
    // point move?
    if (!updated.getBounds().equals(original.getBounds())) {
      components.remove(original, this);
      components.writeCommit(updated, new GeoWaveEmptyTransaction(components));
    } else {
      components.writeCommit(updated, new GeoWaveEmptyTransaction(components));
    }

    final ReferencedEnvelope bounds = new ReferencedEnvelope();
    bounds.include(updated.getBounds());
    bounds.include(original.getBounds());
    components.getGTstore().getListenerManager().fireFeaturesChanged(
        updated.getFeatureType().getTypeName(),
        Transaction.AUTO_COMMIT,
        bounds,
        true);
  }

  @Override
  public void add(final String fid, final SimpleFeature feature) throws IOException {
    feature.getUserData().put(Hints.USE_PROVIDED_FID, true);
    if (feature.getUserData().containsKey(Hints.PROVIDED_FID)) {
      final String providedFid = (String) feature.getUserData().get(Hints.PROVIDED_FID);
      feature.getUserData().put(Hints.PROVIDED_FID, providedFid);
    } else {
      feature.getUserData().put(Hints.PROVIDED_FID, feature.getID());
    }
    components.writeCommit(feature, this);

    components.getGTstore().getListenerManager().fireFeaturesAdded(
        components.getAdapter().getFeatureType().getTypeName(),
        Transaction.AUTO_COMMIT,
        ReferencedEnvelope.reference(feature.getBounds()),
        true);
  }

  @Override
  public void remove(final String fid, final SimpleFeature feature) throws IOException {
    components.remove(feature, this);
    components.getGTstore().getListenerManager().fireFeaturesRemoved(
        feature.getFeatureType().getTypeName(),
        Transaction.AUTO_COMMIT,
        ReferencedEnvelope.reference(feature.getBounds()),
        true);
  }

  public String getID() {
    return "";
  }

  @Override
  public CloseableIterator<SimpleFeature> interweaveTransaction(
      final Integer limit,
      final Filter filter,
      final CloseableIterator<SimpleFeature> it) {
    return it;
  }

  @Override
  public String[] composeAuthorizations() {
    return components.getGTstore().getAuthorizationSPI().getAuthorizations();
  }

  @Override
  public String composeVisibility() {
    return "";
  }
}
