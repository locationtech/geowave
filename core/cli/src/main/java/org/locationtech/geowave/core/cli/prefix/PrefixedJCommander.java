/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.IDefaultProvider;
import com.beust.jcommander.JCommander;

/**
 * This special JCommander instance does two things: 1. It initializes special Prefixed argument
 * objects (via addPrefixedObject) and adds them to the JCommanders object list before parsing 2. It
 * overrides the sub commands that are added to make them instances of PrefixedJCommander 3. It
 * lazily initializes child commands using an Initializer interface.
 */
public class PrefixedJCommander extends JCommander {

  private static Logger LOGGER = LoggerFactory.getLogger(PrefixedJCommander.class);

  // Allows us to override the commanders list that's being stored
  // in our parent class.
  private Map<Object, JCommander> childCommanders;

  // A list of objects to add to the translator before feeding
  // into the internal JCommander object.
  private List<Object> prefixedObjects = null;

  private boolean validate = true;
  private boolean allowUnknown = false;
  private IDefaultProvider defaultProvider = null;

  // The map used to translate the variables back and forth.
  private JCommanderTranslationMap translationMap = null;

  // The initializer is used before parse to allow the user
  // to add additional commands/objects to this commander before
  // it is used
  private PrefixedJCommanderInitializer initializer = null;
  private boolean initialized = false;

  /**
   * Creates a new instance of this commander.
   */
  @SuppressWarnings("unchecked")
  public PrefixedJCommander() {
    super();
    Field commandsField;
    try {
      // HP Fortify "Access Specifier Manipulation"
      // This field is being modified by trusted code,
      // in a way that is not influenced by user input
      commandsField = JCommander.class.getDeclaredField("commands");
      commandsField.setAccessible(true);
      childCommanders = (Map<Object, JCommander>) commandsField.get(this);
    } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
      // This is a programmer error, and will only happen if another
      // version of JCommander is being used.
      // newer versions of JCommander have renamed the member variables, try the old names
      try {
        commandsField = JCommander.class.getDeclaredField("m_commands");

        commandsField.setAccessible(true);
        childCommanders = (Map<Object, JCommander>) commandsField.get(this);
      } catch (final NoSuchFieldException | IllegalArgumentException | IllegalAccessException e2) {
        LOGGER.error("Another version of JCommander is being used", e2);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * This function will translate the given prefixed objects into the object list before parsing.
   * This is so that their descriptions will be picked up.
   */
  private void initialize() {
    if (!initialized) {
      if (translationMap != null) {
        throw new RuntimeException("This PrefixedJCommander has already been used.");
      }

      // Initialize
      if (initializer != null) {
        initializer.initialize(this);
      }

      final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();

      // And these are the input to the translator!
      if (prefixedObjects != null) {
        for (final Object obj : prefixedObjects) {
          translator.addObject(obj);
        }
      }

      translationMap = translator.translate();
      translationMap.createFacadeObjects();

      for (final Object obj : translationMap.getObjects()) {
        addObject(obj);
      }

      // Copy default parameters over for parsing.
      translationMap.transformToFacade();
      initialized = true;
    }
  }

  @Override
  public void addCommand(final String name, final Object object, final String... aliases) {
    super.addCommand(name, new Object(), aliases);

    // Super annoying. Can't control creation of JCommander objects, so
    // just replace it.

    final Iterator<Entry<Object, JCommander>> iter = childCommanders.entrySet().iterator();
    Entry<Object, JCommander> last = null;
    while (iter.hasNext()) {
      last = iter.next();
    }

    final PrefixedJCommander comm = new PrefixedJCommander();
    comm.setProgramName(name, aliases);
    comm.setDefaultProvider(defaultProvider);
    comm.setAcceptUnknownOptions(allowUnknown);
    comm.setValidate(validate);

    if (object != null) {
      comm.addPrefixedObject(object);
    }

    if (last != null) {
      childCommanders.put(last.getKey(), comm);
    }
  }

  @Override
  public void createDescriptions() {
    // because child commanders are called from a private method parseValues() L796 of JCommander
    // v1.78, children don't get initialized without this override
    initialize();
    super.createDescriptions();
  }

  @Override
  public void parse(final String... args) {
    initialize();
    if (validate) {
      super.parse(args);
    } else {
      super.parseWithoutValidation(args);
    }

    complete();
  }

  private void complete() {
    if (initialized) {
      for (JCommander child : childCommanders.values()) {
        if (child instanceof PrefixedJCommander) {
          ((PrefixedJCommander) child).complete();
        }
      }
      translationMap.transformToOriginal();
      translationMap = null;
      initialized = false;
    }
  }

  /**
   * We replace the parseWithoutValidation() command with the setValidate option that we apply to
   * all children. This is because of bug #267 in JCommander.
   */
  @Override
  public void parseWithoutValidation(final String... args) {
    throw new NotImplementedException("Do not use this method.  Use setValidate()");
  }

  @Override
  public void setDefaultProvider(final IDefaultProvider defaultProvider) {
    super.setDefaultProvider(defaultProvider);
    this.defaultProvider = defaultProvider;
  }

  @Override
  public void setAcceptUnknownOptions(final boolean allowUnknown) {
    super.setAcceptUnknownOptions(allowUnknown);
    this.allowUnknown = allowUnknown;
  }

  public void setValidate(final boolean validate) {
    this.validate = validate;
  }

  public List<Object> getPrefixedObjects() {
    return prefixedObjects;
  }

  public void addPrefixedObject(final Object object) {
    if (prefixedObjects == null) {
      prefixedObjects = new ArrayList<>();
    }
    prefixedObjects.add(object);
  }

  public JCommanderTranslationMap getTranslationMap() {
    return translationMap;
  }

  public PrefixedJCommanderInitializer getInitializer() {
    return initializer;
  }

  public void setInitializer(final PrefixedJCommanderInitializer initializer) {
    this.initializer = initializer;
  }

  public interface PrefixedJCommanderInitializer {
    void initialize(PrefixedJCommander commander);
  }
}
