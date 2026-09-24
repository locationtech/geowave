/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.persist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PersistableFactoryTest {
  private static final short ID = 32000;
  private static final short OTHER_ID = 32001;

  private final List<String> errors = new ArrayList<>();
  private final Logger logger = (Logger) LogManager.getLogger(PersistableFactory.class);
  private final AbstractAppender appender =
      new AbstractAppender("PersistableFactoryTest", null, null, false, Property.EMPTY_ARRAY) {
        @Override
        public void append(final LogEvent event) {
          errors.add(event.getMessage().getFormattedMessage());
        }
      };

  @Before
  public void captureErrors() {
    appender.start();
    logger.addAppender(appender);
  }

  @After
  public void stopCapturingErrors() {
    logger.removeAppender(appender);
    appender.stop();
  }

  @Test
  public void testIdCollisionNamesBothClassesAndRegistries() {
    final PersistableFactory factory = new PersistableFactory();
    factory.addRegistry(new FirstRegistry());
    factory.addRegistry(new CollidingRegistry());

    assertTrue(factory.newInstance(ID) instanceof FirstPersistable);
    assertFalse(factory.getClassIdMapping().containsKey(SecondPersistable.class));
    assertEquals(errors.toString(), 1, errors.size());
    final String error = errors.get(0);
    assertTrue(error, error.contains(FirstPersistable.class.getName()));
    assertTrue(error, error.contains(FirstRegistry.class.getName()));
    assertTrue(error, error.contains(SecondPersistable.class.getName()));
    assertTrue(error, error.contains(CollidingRegistry.class.getName()));
  }

  @Test
  public void testClassRegisteredTwiceNamesBothRegistries() {
    final PersistableFactory factory = new PersistableFactory();
    factory.addRegistry(new FirstRegistry());
    factory.addRegistry(new ReregisteringRegistry());

    assertEquals(Short.valueOf(ID), factory.getClassIdMapping().get(FirstPersistable.class));
    assertNull(factory.newInstance(OTHER_ID));
    assertEquals(errors.toString(), 1, errors.size());
    final String error = errors.get(0);
    assertTrue(error, error.contains(FirstPersistable.class.getName()));
    assertTrue(error, error.contains(FirstRegistry.class.getName()));
    assertTrue(error, error.contains(ReregisteringRegistry.class.getName()));
  }

  public static class FirstPersistable implements Persistable {
    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}
  }

  public static class SecondPersistable extends FirstPersistable {
  }

  private static class FirstRegistry implements
      PersistableRegistrySpi,
      InternalPersistableRegistry {
    @Override
    public PersistableIdAndConstructor[] getSupportedPersistables() {
      return new PersistableIdAndConstructor[] {
          new PersistableIdAndConstructor(ID, FirstPersistable::new)};
    }
  }

  private static class CollidingRegistry implements
      PersistableRegistrySpi,
      InternalPersistableRegistry {
    @Override
    public PersistableIdAndConstructor[] getSupportedPersistables() {
      return new PersistableIdAndConstructor[] {
          new PersistableIdAndConstructor(ID, SecondPersistable::new)};
    }
  }

  private static class ReregisteringRegistry implements
      PersistableRegistrySpi,
      InternalPersistableRegistry {
    @Override
    public PersistableIdAndConstructor[] getSupportedPersistables() {
      return new PersistableIdAndConstructor[] {
          new PersistableIdAndConstructor(OTHER_ID, FirstPersistable::new)};
    }
  }
}
