/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

/**
 * This is responsible for persisting adapter/Internal Adapter mappings (either in memory or to disk
 * depending on the implementation).
 */
public interface InternalAdapterStore {

  public String[] getTypeNames();

  public short[] getAdapterIds();

  public String getTypeName(short adapterId);

  public Short getAdapterId(String typeName);

  public short getInitialAdapterId(String typeName);

  /**
   * If an adapter is already associated with an internal Adapter returns false. Adapter can only be
   * associated with internal adapter once.
   *
   * @param typeName the type to add
   * @return the internal ID
   */
  public short addTypeName(String typeName);

  /**
   * Remove a mapping from the store by type name.
   *
   * @param typeName the type to remove
   */
  public boolean remove(String typeName);

  /**
   * Remove a mapping from the store by internal adapter ID.
   * 
   * @param adapterId the internal adapter ID of the adapter to remove
   * @return {@code true} if the type was removed
   */
  public boolean remove(short adapterId);

  /**
   * Remove all mappings from the store.
   */
  public void removeAll();
}
