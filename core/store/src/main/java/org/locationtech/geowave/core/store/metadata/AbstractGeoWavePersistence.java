/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import java.io.IOException;
import java.nio.charset.Charset;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * This abstract class does most of the work for storing persistable objects in Geowave datastores
 * and can be easily extended for any object that needs to be persisted.
 *
 * <p> There is an LRU cache associated with it so staying in sync with external updates is not
 * practical - it assumes the objects are not updated often or at all. The objects are stored in
 * their own table.
 *
 * @param <T> The type of persistable object that this stores
 */
public abstract class AbstractGeoWavePersistence<T extends Persistable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWavePersistence.class);

  // TODO: should we concern ourselves with multiple distributed processes
  // updating and looking up objects simultaneously that would require some
  // locking/synchronization mechanism, and even possibly update
  // notifications?
  protected static final int MAX_ENTRIES = 1000;
  public static final String METADATA_TABLE = "GEOWAVE_METADATA";
  protected final DataStoreOperations operations;
  protected final DataStoreOptions options;
  protected final MetadataType type;

  @SuppressWarnings("rawtypes")
  protected Cache cache;

  public AbstractGeoWavePersistence(
      final DataStoreOperations operations,
      final DataStoreOptions options,
      final MetadataType type) {
    this.operations = operations;
    this.options = options;
    this.type = type;
    buildCache();
  }

  protected void buildCache() {
    final Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().maximumSize(MAX_ENTRIES);
    this.cache = cacheBuilder.<ByteArray, T>build();
  }

  protected MetadataType getType() {
    return type;
  }

  protected ByteArray getSecondaryId(final T persistedObject) {
    // this is the default implementation, if the persistence store requires
    // secondary indices, it needs to override this method
    return null;
  }

  protected abstract ByteArray getPrimaryId(final T persistedObject);

  public void removeAll() {
    deleteObject(null, null);
    cache.invalidateAll();
  }

  protected ByteArray getCombinedId(final ByteArray primaryId, final ByteArray secondaryId) {
    // the secondaryId is optional so check for null or empty
    if ((secondaryId != null)
        && (secondaryId.getBytes() != null)
        && (secondaryId.getBytes().length > 0)) {
      return new ByteArray(primaryId.getString() + "_" + secondaryId.getString());
    }
    return primaryId;
  }

  @SuppressWarnings("unchecked")
  protected void addObjectToCache(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final T object,
      final String... authorizations) {
    final ByteArray combinedId = getCombinedId(primaryId, secondaryId);
    cache.put(combinedId, object);
  }

  protected Object getObjectFromCache(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final String... authorizations) {
    final ByteArray combinedId = getCombinedId(primaryId, secondaryId);
    return cache.getIfPresent(combinedId);
  }

  protected boolean deleteObjectFromCache(final ByteArray primaryId, final ByteArray secondaryId) {
    final ByteArray combinedId = getCombinedId(primaryId, secondaryId);
    if (combinedId != null) {
      final boolean present = cache.getIfPresent(combinedId) != null;
      if (present) {
        cache.invalidate(combinedId);
      }
      return present;
    }
    return false;
  }

  public void remove(final ByteArray adapterId) {
    deleteObject(adapterId, null);
  }

  protected boolean deleteObject(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final String... authorizations) {
    if (deleteObjects(primaryId, secondaryId, authorizations)) {
      deleteObjectFromCache(primaryId, secondaryId);
      return true;
    }
    return false;
  }

  protected void addObject(final T object) {
    addObject(getPrimaryId(object), getSecondaryId(object), object);
  }

  protected byte[] getVisibility(final T entry) {
    return null;
  }

  protected byte[] toBytes(final String s) {
    if (s == null) {
      return null;
    }
    return s.getBytes(Charset.forName("UTF-8"));
  }

  protected void addObject(final ByteArray id, final ByteArray secondaryId, final T object) {
    addObjectToCache(id, secondaryId, object);
    try (final MetadataWriter writer = operations.createMetadataWriter(getType())) {
      if (writer != null) {
        final GeoWaveMetadata metadata =
            new GeoWaveMetadata(
                id.getBytes(),
                secondaryId != null ? secondaryId.getBytes() : null,
                getVisibility(object),
                getValue(object));
        writer.write(metadata);
      }
    } catch (final Exception e) {
      LOGGER.warn("Unable to close metadata writer", e);
      e.printStackTrace();
    }
  }

  protected byte[] getValue(final T object) {
    final byte[] value = PersistenceUtils.toBinary(object);
    if ((object != null) && ((value == null) || (value.length == 0))) {
      throw new UnsupportedOperationException(
          "Object of class "
              + object.getClass().getName()
              + " was not found in the persistable registry and cannot be persisted!");
    }
    return value;
  }

  protected CloseableIterator<T> getAllObjectsWithSecondaryId(
      final ByteArray secondaryId,
      final String... authorizations) {
    return internalGetObjects(new MetadataQuery(secondaryId.getBytes(), authorizations));
  }

  protected T getObject(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final String... authorizations) {
    return internalGetObject(primaryId, secondaryId, true, authorizations);
  }

  @SuppressWarnings("unchecked")
  protected T internalGetObject(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final boolean warnIfNotExists,
      final String... authorizations) {
    final Object cacheResult = getObjectFromCache(primaryId, secondaryId, authorizations);
    if (cacheResult != null) {
      return (T) cacheResult;
    }

    try {
      if (!operations.metadataExists(getType())) {
        if (warnIfNotExists) {
          LOGGER.warn(
              "Object '"
                  + getCombinedId(primaryId, secondaryId).getString()
                  + "' not found. '"
                  + METADATA_TABLE
                  + "' table does not exist");
        }
        return null;
      }
    } catch (final IOException e1) {
      if (warnIfNotExists) {
        LOGGER.error("Unable to check for existence of metadata to get object", e1);
      }
      return null;
    }
    final MetadataReader reader = operations.createMetadataReader(getType());
    try (final CloseableIterator<GeoWaveMetadata> it =
        reader.query(
            new MetadataQuery(
                primaryId.getBytes(),
                secondaryId == null ? null : secondaryId.getBytes(),
                authorizations))) {
      if (!it.hasNext()) {
        if (warnIfNotExists) {
          LOGGER.warn(
              "Object '" + getCombinedId(primaryId, secondaryId).getString() + "' not found");
        }
        return null;
      }
      final GeoWaveMetadata entry = it.next();
      return entryToValue(entry, authorizations);
    }
  }

  protected boolean objectExists(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final String... authorizations) {
    return internalGetObject(primaryId, secondaryId, false, authorizations) != null;
  }

  protected CloseableIterator<T> getObjects(final String... authorizations) {
    return internalGetObjects(new MetadataQuery(null, authorizations));
  }

  protected CloseableIterator<T> internalGetObjects(final MetadataQuery query) {
    try {
      if (!operations.metadataExists(getType())) {
        return new CloseableIterator.Empty<>();
      }
    } catch (final IOException e1) {
      LOGGER.error("Unable to check for existence of metadata to get objects", e1);
      return new CloseableIterator.Empty<>();
    }
    final MetadataReader reader = operations.createMetadataReader(getType());
    final CloseableIterator<GeoWaveMetadata> it = reader.query(query);
    return new NativeIteratorWrapper(it, query.getAuthorizations());
  }

  @SuppressWarnings("unchecked")
  protected T fromValue(final GeoWaveMetadata entry) {
    return (T) PersistenceUtils.fromBinary(entry.getValue());
  }

  protected T entryToValue(final GeoWaveMetadata entry, final String... authorizations) {
    final T result = fromValue(entry);
    if (result != null) {
      addObjectToCache(
          new ByteArray(entry.getPrimaryId()),
          (entry.getSecondaryId() == null) || (entry.getSecondaryId().length == 0) ? null
              : new ByteArray(entry.getSecondaryId()),
          result,
          authorizations);
    }
    return result;
  }

  public boolean deleteObjects(final ByteArray secondaryId, final String... authorizations) {
    return deleteObjects(null, secondaryId, authorizations);
  }

  public boolean deleteObjects(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final String... authorizations) {
    return deleteObjects(primaryId, secondaryId, operations, getType(), this, authorizations);
  }

  protected static <T extends Persistable> boolean deleteObjects(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final DataStoreOperations operations,
      final MetadataType type,
      final AbstractGeoWavePersistence<T> cacheDeleter,
      final String... authorizations) {
    return deleteObjects(
        primaryId,
        secondaryId,
        false,
        operations,
        type,
        cacheDeleter,
        authorizations);
  }

  protected static <T extends Persistable> boolean deleteObjects(
      final ByteArray primaryId,
      final ByteArray secondaryId,
      final boolean primaryIdPrefix,
      final DataStoreOperations operations,
      final MetadataType type,
      final AbstractGeoWavePersistence<T> cacheDeleter,
      final String... authorizations) {
    try {
      if (!operations.metadataExists(type)) {
        return false;
      }
    } catch (final IOException e1) {
      LOGGER.error("Unable to check for existence of metadata to delete objects", e1);
      return false;
    }
    try (final MetadataDeleter deleter = operations.createMetadataDeleter(type)) {
      if ((primaryId == null) && (secondaryId == null)) {
        // this is trying to delete everything, let's clear the cache (although there's an
        // off-chance authorizations might not force the entire stats to be cleared, the cache is
        // merely a performance optimization)
        if (cacheDeleter != null) {
          cacheDeleter.cache.invalidateAll();
        }
        return deleter.delete(new MetadataQuery((byte[]) null, (byte[]) null, authorizations));
      }
      boolean retVal = false;
      final MetadataReader reader = operations.createMetadataReader(type);
      try (final CloseableIterator<GeoWaveMetadata> it =
          reader.query(
              new MetadataQuery(
                  primaryId != null ? primaryId.getBytes() : null,
                  secondaryId != null ? secondaryId.getBytes() : null,
                  primaryIdPrefix,
                  authorizations))) {

        while (it.hasNext()) {
          retVal = true;
          final GeoWaveMetadata entry = it.next();
          if (cacheDeleter != null) {
            cacheDeleter.deleteObjectFromCache(
                new ByteArray(entry.getPrimaryId()),
                new ByteArray(entry.getSecondaryId()));
          }
          deleter.delete(
              new MetadataQuery(entry.getPrimaryId(), entry.getSecondaryId(), authorizations));
        }
      }
      return retVal;
    } catch (final Exception e) {
      LOGGER.error("Unable to delete objects", e);
    }
    return false;
  }

  private class NativeIteratorWrapper implements CloseableIterator<T> {
    private final CloseableIterator<GeoWaveMetadata> it;
    private final String[] authorizations;

    private NativeIteratorWrapper(
        final CloseableIterator<GeoWaveMetadata> it,
        final String[] authorizations) {
      this.it = it;
      this.authorizations = authorizations;
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public T next() {
      return entryToValue(it.next(), authorizations);
    }

    @Override
    public void remove() {
      it.remove();
    }

    @Override
    public void close() {
      it.close();
    }
  }
}
