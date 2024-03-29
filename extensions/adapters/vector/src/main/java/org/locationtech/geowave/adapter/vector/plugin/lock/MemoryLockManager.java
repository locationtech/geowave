/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/** Single GeoServer lock support. In a clustered model, do not use. */
public class MemoryLockManager extends AbstractLockingManagement {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryLockManager.class);
  private static final Map<String, Map<String, AuthorizedLock>> LOCKS = new HashMap<>();
  private final Map<String, AuthorizedLock> locks;

  public MemoryLockManager(final String instanceName) {
    Map<String, AuthorizedLock> lockSet;
    synchronized (LOCKS) {
      lockSet = LOCKS.get(instanceName);
      if (lockSet == null) {
        lockSet = new HashMap<>();
        LOCKS.put(instanceName, lockSet);
      }
    }
    locks = lockSet;
  }

  public MemoryLockManager(final GeoWavePluginConfig pluginConfig) {
    this(pluginConfig.getName());
  }

  @Override
  public void releaseAll(final AuthorizedLock lock) {
    final ArrayList<AuthorizedLock> toRelease = new ArrayList<>();
    synchronized (locks) {
      final Iterator<Entry<String, AuthorizedLock>> it = locks.entrySet().iterator();
      while (it.hasNext()) {
        final Entry<String, AuthorizedLock> entry = it.next();
        if (entry.getValue().equals(lock) || entry.getValue().isAuthorized(lock)) {
          toRelease.add(entry.getValue());
          it.remove();
        }
      }
    }
    for (final AuthorizedLock lockToRelease : toRelease) {
      lockToRelease.invalidate();
    }
  }

  /**
   * Release all locks associated with a transaction. Occurs on commit and rollback
   *
   * @param lock
   */
  @Override
  public void resetAll(final AuthorizedLock lock) {
    final ArrayList<AuthorizedLock> toRelease = new ArrayList<>();
    synchronized (locks) {
      final Iterator<Entry<String, AuthorizedLock>> it = locks.entrySet().iterator();
      while (it.hasNext()) {
        final Entry<String, AuthorizedLock> entry = it.next();
        if (entry.getValue().equals(lock) || entry.getValue().isAuthorized(lock)) {
          toRelease.add(entry.getValue());
        }
      }
    }
    for (final AuthorizedLock lockToRelease : toRelease) {
      lockToRelease.resetExpireTime();
    }
  }

  @SuppressFBWarnings(
      value = {"MWN_MISMATCHED_WAIT"},
      justification = "incorrect flag; lock held (in synchronized block)")
  @Override
  public void lock(final AuthorizedLock lock, final String featureID) {
    AuthorizedLock featureLock = null;

    synchronized (locks) {
      featureLock = locks.get(featureID);
      if ((featureLock == null) || featureLock.isStale()) {
        featureLock = lock;
        locks.put(featureID, lock);
        return;
      } else if (featureLock.isAuthorized(lock)) {
        return;
      }
    }
    // want to loop until this 'lock' is the 'winning' lock.
    while (featureLock != lock) {
      // at this point, some other transaction may have the lock
      synchronized (featureLock) {
        // check if stale, which occurs when the transaction is
        // completed.
        while (!featureLock.isStale()) {
          try {
            // only wait a little, because the feature lock could be
            // stale
            // flagged as mismatched wait...but this is correct
            featureLock.wait(
                Math.min(5000, featureLock.getExpireTime() - System.currentTimeMillis()));
          } catch (final InterruptedException ex) {
          } catch (final Exception e) {
            LOGGER.error(
                "Memory lock manager filed to wait for lock release. Will cycle till lock is stale.",
                e);
          }
        }
      }
      synchronized (locks) {
        featureLock = locks.get(featureID);
        // did this code win the race to get the lock for the feature
        // ID?
        if ((featureLock == null) || featureLock.isStale()) {
          locks.put(featureID, lock);
          featureLock = lock;
        }
      }
    }
  }

  @Override
  public boolean exists(final String authID) {
    synchronized (locks) {
      final Iterator<Entry<String, AuthorizedLock>> it = locks.entrySet().iterator();
      while (it.hasNext()) {
        final Entry<String, AuthorizedLock> entry = it.next();
        if (entry.getValue().isAuthorized(authID) || !entry.getValue().isStale()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void unlock(final AuthorizedLock lock, final String featureID) {
    AuthorizedLock featureLock = null;
    boolean notify = false;
    synchronized (locks) {
      featureLock = locks.get(featureID);
      if ((featureLock != null) && featureLock.isAuthorized(lock)) {
        locks.remove(featureID);
        notify = true;
      }
    }
    if (notify) {
      featureLock.invalidate();
    }
  }
}
