/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.vector.plugin.visibility.VisibilityConfiguration;
import org.locationtech.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import org.locationtech.geowave.adapter.vector.stats.StatsManager;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.dimension.Time;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.InitializeWithIndicesDataAdapter;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.visibility.VisibilityManagement;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.HadoopDataAdapter;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializer;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * This data adapter will handle all reading/writing concerns for storing and retrieving GeoTools
 * SimpleFeature objects to and from a GeoWave persistent store in Accumulo. <br> <br> If the
 * implementor needs to write rows with particular visibility, this can be done by providing a
 * FieldVisibilityHandler to a constructor or a VisibilityManagement to a constructor. When using
 * VisibilityManagement, the feature attribute to contain the visibility meta-data is, by default,
 * called GEOWAVE_VISIBILITY. This can be overridden by setting the UserData property 'visibility'
 * to Boolean.TRUE for the feature attribute that describes the attribute that contains the
 * visibility meta-data. persistedType.getDescriptor("someAttributeName").getUserData().put(
 * "visibility", Boolean.TRUE)<br> <br> The adapter will use the SimpleFeature's default geometry
 * for spatial indexing.<br> <br> The adaptor will use the first temporal attribute (a Calendar or
 * Date object) as the timestamp of a temporal index.<br> <br> If the feature type contains a
 * UserData property 'time' for a specific time attribute with Boolean.TRUE, then the attribute is
 * used as the timestamp of a temporal index.<br> <br> If the feature type contains UserData
 * properties 'start' and 'end' for two different time attributes with value Boolean.TRUE, then the
 * attributes are used for a range index.<br> <br> If the feature type contains a UserData property
 * 'time' for *all* time attributes with Boolean.FALSE, then a temporal index is not used.<br> <br>
 * Statistics configurations are maintained in UserData. Each attribute may have a UserData property
 * called 'stats'. The associated value is an instance of
 * {@link org.locationtech.geowave.adapter.vector.stats.StatsConfigurationCollection} . The
 * collection maintains a set of {@link org.locationtech.geowave.adapter.vector.stats.StatsConfig},
 * one for each type of statistic. The default statistics for geometry and temporal constraints
 * cannot be changed, as they are critical components to the efficiency of query processing.
 */
public class FeatureDataAdapter extends AbstractDataAdapter<SimpleFeature> implements
    GeotoolsFeatureDataAdapter<SimpleFeature>,
    StatisticsProvider<SimpleFeature>,
    HadoopDataAdapter<SimpleFeature, FeatureWritable>,
    InitializeWithIndicesDataAdapter<SimpleFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDataAdapter.class);
  // the original coordinate system will always be represented internally by
  // the persisted type
  private SimpleFeatureType persistedFeatureType;

  // externally the reprojected type will always be advertised because all
  // features will be reprojected to EPSG:4326 and the advertised feature type
  // from the data adapter should match in CRS
  private SimpleFeatureType reprojectedFeatureType;
  private MathTransform transform;
  private StatsManager statsManager;
  private TimeDescriptors timeDescriptors = null;

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  protected FeatureDataAdapter() {}

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Constructor<br> Creates a FeatureDataAdapter for the specified SimpleFeatureType
   *
   * @param featureType - feature type for this object
   */
  public FeatureDataAdapter(final SimpleFeatureType featureType) {
    this(
        featureType,
        new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
        null,
        null);
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Constructor<br> Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
   * provided customIndexHandlers
   *
   * @param featureType - feature type for this object
   * @param customIndexHandlers - l
   */
  public FeatureDataAdapter(
      final SimpleFeatureType featureType,
      final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers) {
    this(featureType, customIndexHandlers, null, null);
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Constructor<br> Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
   * provided visibilityManagement
   *
   * @param featureType - feature type for this object
   * @param visibilityManagement
   */
  public FeatureDataAdapter(
      final SimpleFeatureType featureType,
      final VisibilityManagement<SimpleFeature> visibilityManagement) {
    this(
        featureType,
        new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
        null,
        visibilityManagement);
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Constructor<br> Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
   * provided fieldVisiblityHandler
   *
   * @param featureType - feature type for this object
   * @param fieldVisiblityHandler
   */
  public FeatureDataAdapter(
      final SimpleFeatureType featureType,
      final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler) {
    this(
        featureType,
        new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
        fieldVisiblityHandler,
        null);
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------
  /**
   * Constructor<br> Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
   * provided customIndexHandlers, fieldVisibilityHandler and defaultVisibilityManagement
   *
   * @param featureType - feature type for this object
   * @param customIndexHandlers
   * @param fieldVisiblityHandler
   * @param defaultVisibilityManagement
   */
  public FeatureDataAdapter(
      final SimpleFeatureType featureType,
      final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers,
      final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler,
      final VisibilityManagement<SimpleFeature> defaultVisibilityManagement) {

    super(
        customIndexHandlers,
        new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(),
        fieldVisiblityHandler,
        updateVisibility(featureType, defaultVisibilityManagement));
    setFeatureType(featureType);
  }

  @Override
  public boolean init(final Index... indices) throws RuntimeException {
    String indexCrsCode =
        reprojectedFeatureType == null ? null
            : GeometryUtils.getCrsCode(reprojectedFeatureType.getCoordinateReferenceSystem());
    for (final Index primaryindx : indices) {
      // for first iteration
      if (indexCrsCode == null) {
        if (primaryindx.getIndexModel() instanceof CustomCrsIndexModel) {
          indexCrsCode = ((CustomCrsIndexModel) primaryindx.getIndexModel()).getCrsCode();
        } else {
          indexCrsCode = GeometryUtils.DEFAULT_CRS_STR;
        }
      } else {
        if (primaryindx.getIndexModel() instanceof CustomCrsIndexModel) {
          // check if indexes have different CRS
          if (!indexCrsCode.equals(
              ((CustomCrsIndexModel) primaryindx.getIndexModel()).getCrsCode())) {
            LOGGER.error("Multiple indices with different CRS is not supported");
            throw new RuntimeException("Multiple indices with different CRS is not supported");
          }
        } else {
          if (!indexCrsCode.equals(GeometryUtils.DEFAULT_CRS_STR)) {
            LOGGER.error("Multiple indices with different CRS is not supported");
            throw new RuntimeException("Multiple indices with different CRS is not supported");
          }
        }
      }
    }
    if (reprojectedFeatureType != null) {
      return false;
    }
    initCRS(indexCrsCode);
    return true;
  }

  private void initCRS(String indexCrsCode) {
    if ((indexCrsCode == null) || indexCrsCode.isEmpty()) {
      indexCrsCode = GeometryUtils.DEFAULT_CRS_STR;
    }
    CoordinateReferenceSystem persistedCRS = persistedFeatureType.getCoordinateReferenceSystem();

    if (persistedCRS == null) {
      persistedCRS = GeometryUtils.getDefaultCRS();
    }

    final CoordinateReferenceSystem indexCRS = decodeCRS(indexCrsCode);
    if (indexCRS.equals(persistedCRS)) {
      reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(persistedFeatureType, persistedCRS);
      transform = null;
    } else {
      reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(persistedFeatureType, indexCRS);
      try {
        transform = CRS.findMathTransform(persistedCRS, indexCRS, true);
        if (transform.isIdentity()) {
          transform = null;
        }
      } catch (final FactoryException e) {
        LOGGER.warn("Unable to create coordinate reference system transform", e);
      }
    }

    statsManager = new StatsManager(this, persistedFeatureType, reprojectedFeatureType, transform);
  }

  /** Helper method for establishing a visibility manager in the constructor */
  private static SimpleFeatureType updateVisibility(
      final SimpleFeatureType featureType,
      final VisibilityManagement<SimpleFeature> defaultVisibilityManagement) {
    final VisibilityConfiguration config = new VisibilityConfiguration(featureType);
    config.updateWithDefaultIfNeeded(featureType, defaultVisibilityManagement);

    return featureType;
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Set the FeatureType for this Data Adapter.
   *
   * @param featureType - new feature type
   */
  private void setFeatureType(final SimpleFeatureType featureType) {
    persistedFeatureType = featureType;
    resetTimeDescriptors();
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Create List of NativeFieldHandlers based on the SimpleFeature type passed as parameter.
   *
   * @param featureType - SFT to be used to determine handlers
   * @return List of NativeFieldHandlers that correspond to attributes in featureType
   */
  protected List<NativeFieldHandler<SimpleFeature, Object>> getFieldHandlersFromFeatureType(
      final SimpleFeatureType featureType) {
    final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers =
        new ArrayList<>(featureType.getAttributeCount());

    for (final AttributeDescriptor attrDesc : featureType.getAttributeDescriptors()) {
      nativeHandlers.add(new FeatureAttributeHandler(attrDesc));
    }

    return nativeHandlers;
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Attempts to find a time descriptor (range or timestamp) within provided featureType and return
   * index field handler for it.
   *
   * @param featureType - feature type to be scanned.
   * @return Index Field Handler for the time descriptor found in featureType
   */
  protected IndexFieldHandler<SimpleFeature, Time, Object> getTimeRangeHandler(
      final SimpleFeatureType featureType) {
    final VisibilityConfiguration config = new VisibilityConfiguration(featureType);
    final TimeDescriptors timeDescriptors = TimeUtils.inferTimeAttributeDescriptor(featureType);

    if ((timeDescriptors.getStartRange() != null) && (timeDescriptors.getEndRange() != null)) {

      final FeatureAttributeHandler fah_startRange =
          new FeatureAttributeHandler(timeDescriptors.getStartRange());
      final FeatureAttributeHandler fah_endRange =
          new FeatureAttributeHandler(timeDescriptors.getEndRange());
      final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler =
          config.getManager().createVisibilityHandler(
              timeDescriptors.getStartRange().getLocalName(),
              fieldVisiblityHandler,
              config.getAttributeName());

      final FeatureTimeRangeHandler ftrh =
          new FeatureTimeRangeHandler(fah_startRange, fah_endRange, visibilityHandler);

      return (ftrh);
    } else if (timeDescriptors.getTime() != null) {
      // if we didn't succeed in identifying a start and end time,
      // just grab the first attribute and use it as a timestamp

      final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler =
          config.getManager().createVisibilityHandler(
              timeDescriptors.getTime().getLocalName(),
              fieldVisiblityHandler,
              config.getAttributeName());

      final FeatureTimestampHandler fth =
          new FeatureTimestampHandler(timeDescriptors.getTime(), visibilityHandler);

      return fth;
    }

    return null;
  }

  // -----------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------

  /**
   * Get a List<> of the default index field handlers from the Simple Feature Type provided
   *
   * @param typeObj - Simple Feature Type object
   * @return - List of the default Index Field Handlers
   */
  @Override
  protected List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> getDefaultTypeMatchingHandlers(
      final Object typeObj) {
    if ((typeObj != null) && (typeObj instanceof SimpleFeatureType)) {

      final SimpleFeatureType internalType = (SimpleFeatureType) typeObj;
      final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> defaultHandlers =
          new ArrayList<>();

      nativeFieldHandlers = getFieldHandlersFromFeatureType(internalType);

      // Add default handler for Time

      final IndexFieldHandler<SimpleFeature, Time, Object> timeHandler =
          getTimeRangeHandler(internalType);
      if (timeHandler != null) {
        defaultHandlers.add(timeHandler);
      }

      // Add default handler for Geometry

      final AttributeDescriptor descriptor = internalType.getGeometryDescriptor();
      final VisibilityConfiguration visConfig = new VisibilityConfiguration(internalType);
      if (descriptor != null) {
        defaultHandlers.add(
            new FeatureGeometryHandler(
                descriptor,
                visConfig.getManager().createVisibilityHandler(
                    descriptor.getLocalName(),
                    fieldVisiblityHandler,
                    visConfig.getAttributeName())));
      }
      return defaultHandlers;
    }

    LOGGER.warn("Simple Feature Type could not be used for handling the indexed data");
    return super.getDefaultTypeMatchingHandlers(reprojectedFeatureType);
  }

  @Override
  public IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> getFieldHandler(
      final NumericDimensionField<? extends CommonIndexValue> dimension) {
    if (dimension instanceof FeatureAttributeDimensionField) {
      final VisibilityConfiguration visConfig = new VisibilityConfiguration(reprojectedFeatureType);
      final AttributeDescriptor desc =
          reprojectedFeatureType.getDescriptor(dimension.getFieldName());
      if (desc != null) {
        return new FeatureAttributeCommonIndexFieldHandler(
            desc,
            visConfig.getManager().createVisibilityHandler(
                desc.getLocalName(),
                fieldVisiblityHandler,
                visConfig.getAttributeName()));
      }
    }
    return super.getFieldHandler(dimension);
  }

  /**
   * Sets the namespace of the reprojected feature type associated with this data adapter
   *
   * @param namespaceURI - new namespace URI
   */
  @Override
  public void setNamespace(final String namespaceURI) {
    final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    builder.init(reprojectedFeatureType);
    builder.setNamespaceURI(namespaceURI);
    reprojectedFeatureType = builder.buildFeatureType();
  }

  // ----------------------------------------------------------------------------------
  /** Map of Field Readers associated with a Field ID */
  private final Map<String, FieldReader<Object>> mapOfFieldNameToReaders = new HashMap<>();

  /**
   * {@inheritDoc}
   *
   * @return Field Reader for the given Field ID
   */
  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    // Go to the map to get a reader for given fieldId

    FieldReader<Object> reader = mapOfFieldNameToReaders.get(fieldName);

    // Check the map to see if a reader has already been found.
    if (reader == null) {
      // Reader not in Map, go to the reprojected feature type and get the
      // default reader
      final AttributeDescriptor descriptor = reprojectedFeatureType.getDescriptor(fieldName);
      final Class<?> bindingClass = descriptor.getType().getBinding();
      reader = (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(bindingClass);

      // Add it to map for the next time
      mapOfFieldNameToReaders.put(fieldName, reader);
    }

    return reader;
  }

  // ----------------------------------------------------------------------------------
  /** Map of Field Writers associated with a Field ID */
  private final Map<String, FieldWriter<SimpleFeature, Object>> mapOfFieldNameToWriters =
      new HashMap<>();

  /**
   * {@inheritDoc}
   *
   * @return Field Writer for the given Field ID
   */
  @Override
  public FieldWriter<SimpleFeature, Object> getWriter(final String fieldName) {
    // Go to the map to get a writer for given fieldId

    FieldWriter<SimpleFeature, Object> writer = mapOfFieldNameToWriters.get(fieldName);

    // Check the map to see if a writer has already been found.
    if (writer == null) {
      final FieldVisibilityHandler<SimpleFeature, Object> handler =
          getLocalVisibilityHandler(fieldName);
      final AttributeDescriptor descriptor = reprojectedFeatureType.getDescriptor(fieldName);

      final Class<?> bindingClass = descriptor.getType().getBinding();
      if (handler != null) {
        writer =
            (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(
                bindingClass,
                handler);
      } else {
        writer =
            (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(bindingClass);
      }
      if (writer == null) {
        LOGGER.error("BasicWriter not found for binding type:" + bindingClass.getName().toString());
      }

      mapOfFieldNameToWriters.put(fieldName, writer);
    }
    return writer;
  }

  // ----------------------------------------------------------------------------------

  private FieldVisibilityHandler<SimpleFeature, Object> getLocalVisibilityHandler(
      final String fieldName) {
    final VisibilityConfiguration visConfig = new VisibilityConfiguration(reprojectedFeatureType);

    // See if there is a visibility config stored in the reprojected feature
    // type
    if (reprojectedFeatureType.getDescriptor(visConfig.getAttributeName()) == null) {
      // No, so return the default field visibility handler
      return fieldVisiblityHandler;
    }

    // Yes, then get the descriptor for the given field ID
    final AttributeDescriptor descriptor = reprojectedFeatureType.getDescriptor(fieldName);

    return visConfig.getManager().createVisibilityHandler(
        descriptor.getLocalName(),
        fieldVisiblityHandler,
        visConfig.getAttributeName());
  }

  // ----------------------------------------------------------------------------------

  /**
   * Get feature type default data contained in the reprojected SFT and serialize to a binary stream
   *
   * @return byte array with binary data
   */
  @Override
  protected byte[] defaultTypeDataToBinary() {
    // serialize the persisted/reprojected feature type by using default
    // fields and
    // data types

    final String encodedType = DataUtilities.encodeType(persistedFeatureType);
    final String axis =
        FeatureDataUtils.getAxis(persistedFeatureType.getCoordinateReferenceSystem());
    final String typeName = reprojectedFeatureType.getTypeName();
    final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
    final byte[] axisBytes = StringUtils.stringToBinary(axis);
    //
    final SimpleFeatureUserDataConfigurationSet userDataConfiguration =
        new SimpleFeatureUserDataConfigurationSet();
    userDataConfiguration.addConfigurations(
        typeName,
        new TimeDescriptorConfiguration(persistedFeatureType));
    userDataConfiguration.addConfigurations(
        typeName,
        new SimpleFeatureStatsConfigurationCollection(persistedFeatureType));
    userDataConfiguration.addConfigurations(
        typeName,
        new VisibilityConfiguration(persistedFeatureType));
    final byte[] attrBytes = userDataConfiguration.toBinary();
    final String namespace = reprojectedFeatureType.getName().getNamespaceURI();

    byte[] namespaceBytes;
    if ((namespace != null) && (namespace.length() > 0)) {
      namespaceBytes = StringUtils.stringToBinary(namespace);
    } else {
      namespaceBytes = new byte[0];
    }
    final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
    final CoordinateReferenceSystem crs = reprojectedFeatureType.getCoordinateReferenceSystem();
    final byte[] indexCrsBytes;
    if (crs != null) {
      indexCrsBytes = StringUtils.stringToBinary(CRS.toSRS(crs));
    } else {
      indexCrsBytes = new byte[0];
    }
    // 21 bytes is the 7 four byte length fields and one byte for the
    // version
    final ByteBuffer buf =
        ByteBuffer.allocate(
            encodedTypeBytes.length
                + indexCrsBytes.length
                + typeNameBytes.length
                + namespaceBytes.length
                + attrBytes.length
                + axisBytes.length
                + VarintUtils.unsignedIntByteLength(typeNameBytes.length)
                + VarintUtils.unsignedIntByteLength(indexCrsBytes.length)
                + VarintUtils.unsignedIntByteLength(namespaceBytes.length)
                + VarintUtils.unsignedIntByteLength(attrBytes.length)
                + VarintUtils.unsignedIntByteLength(axisBytes.length)
                + VarintUtils.unsignedIntByteLength(encodedTypeBytes.length));
    VarintUtils.writeUnsignedInt(typeNameBytes.length, buf);
    VarintUtils.writeUnsignedInt(indexCrsBytes.length, buf);
    VarintUtils.writeUnsignedInt(namespaceBytes.length, buf);
    VarintUtils.writeUnsignedInt(attrBytes.length, buf);
    VarintUtils.writeUnsignedInt(axisBytes.length, buf);
    VarintUtils.writeUnsignedInt(encodedTypeBytes.length, buf);
    buf.put(typeNameBytes);
    buf.put(indexCrsBytes);
    buf.put(namespaceBytes);
    buf.put(attrBytes);
    buf.put(axisBytes);
    buf.put(encodedTypeBytes);
    return buf.array();
  }

  /**
   * Extract the feature type default data from the binary stream passed in and place in the
   * reprojected SFT for this feature data adapter
   *
   * @return if successful, the reprojected feature type created from the serialized stream
   */
  @Override
  protected Object defaultTypeDataFromBinary(final byte[] bytes) {
    try {
      GeometryUtils.initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize GeoTools classloader", e);
    }
    // deserialize the feature type
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int typeNameByteLength = VarintUtils.readUnsignedInt(buf);
    final int indexCrsByteLength = VarintUtils.readUnsignedInt(buf);
    final int namespaceByteLength = VarintUtils.readUnsignedInt(buf);

    final int attrByteLength = VarintUtils.readUnsignedInt(buf);
    final int axisByteLength = VarintUtils.readUnsignedInt(buf);
    final int encodedTypeByteLength = VarintUtils.readUnsignedInt(buf);

    final byte[] typeNameBytes = ByteArrayUtils.safeRead(buf, typeNameByteLength);
    final byte[] indexCrsBytes = ByteArrayUtils.safeRead(buf, indexCrsByteLength);
    final byte[] namespaceBytes = ByteArrayUtils.safeRead(buf, namespaceByteLength);
    final byte[] attrBytes = ByteArrayUtils.safeRead(buf, attrByteLength);
    final byte[] axisBytes = ByteArrayUtils.safeRead(buf, axisByteLength);
    final byte[] encodedTypeBytes = ByteArrayUtils.safeRead(buf, encodedTypeByteLength);

    final String typeName = StringUtils.stringFromBinary(typeNameBytes);
    String namespace = StringUtils.stringFromBinary(namespaceBytes);
    if (namespace.length() == 0) {
      namespace = null;
    }

    // 21 bytes is the 7 four byte length fields and one byte for the
    // version
    final byte[] secondaryIndexBytes = new byte[buf.remaining()];
    buf.get(secondaryIndexBytes);

    final String encodedType = StringUtils.stringFromBinary(encodedTypeBytes);
    try {
      final SimpleFeatureType myType =
          FeatureDataUtils.decodeType(
              namespace,
              typeName,
              encodedType,
              StringUtils.stringFromBinary(axisBytes));

      final SimpleFeatureUserDataConfigurationSet userDataConfiguration =
          new SimpleFeatureUserDataConfigurationSet();
      userDataConfiguration.addConfigurations(typeName, new TimeDescriptorConfiguration(myType));
      userDataConfiguration.addConfigurations(
          typeName,
          new SimpleFeatureStatsConfigurationCollection(myType));
      userDataConfiguration.addConfigurations(typeName, new VisibilityConfiguration(myType));
      userDataConfiguration.fromBinary(attrBytes);
      userDataConfiguration.updateType(myType);
      setFeatureType(myType);
      initCRS(indexCrsBytes.length > 0 ? StringUtils.stringFromBinary(indexCrsBytes) : null);
      // advertise the reprojected type externally
      return reprojectedFeatureType;
    } catch (final SchemaException e) {
      LOGGER.error("Unable to deserialized feature type", e);
    }
    return null;
  }

  @Override
  public String getTypeName() {
    return persistedFeatureType.getTypeName();
  }

  @Override
  public byte[] getDataId(final SimpleFeature entry) {
    return StringUtils.stringToBinary(entry.getID());
  }

  private ThreadLocal<FeatureRowBuilder> builder = null;

  @Override
  protected RowBuilder<SimpleFeature, Object> newBuilder() {
    if (builder == null) {
      builder = new ThreadLocal<FeatureRowBuilder>() {
        @Override
        protected FeatureRowBuilder initialValue() {
          return internalCreateRowBuilder();
        }
      };
    }
    return builder.get();
  }

  protected FeatureRowBuilder internalCreateRowBuilder() {
    return new FeatureRowBuilder(reprojectedFeatureType);
  }

  @Override
  public SimpleFeatureType getFeatureType() {
    if (reprojectedFeatureType == null) {
      return persistedFeatureType;
    }
    return reprojectedFeatureType;
  }

  @Override
  public AdapterPersistenceEncoding encode(
      final SimpleFeature entry,
      final CommonIndexModel indexModel) {

    if (transform != null) {
      return super.encode(
          GeometryUtils.crsTransform(entry, reprojectedFeatureType, transform),
          indexModel);
    }
    return super.encode(entry, indexModel);
  }

  @Override
  public StatisticsId[] getSupportedStatistics() {
    return statsManager.getSupportedStatistics();
  }

  @Override
  public <R, B extends StatisticsQueryBuilder<R, B>> InternalDataStatistics<SimpleFeature, R, B> createDataStatistics(
      final StatisticsId statisticsId) {
    return (InternalDataStatistics<SimpleFeature, R, B>) statsManager.createDataStatistics(
        statisticsId);
  }

  @Override
  public EntryVisibilityHandler<SimpleFeature> getVisibilityHandler(
      final CommonIndexModel indexModel,
      final DataTypeAdapter<SimpleFeature> adapter,
      final StatisticsId statisticsId) {
    return statsManager.getVisibilityHandler(indexModel, adapter, statisticsId);
  }

  @Override
  public boolean hasTemporalConstraints() {
    return getTimeDescriptors().hasTime();
  }

  public synchronized void resetTimeDescriptors() {
    timeDescriptors = TimeUtils.inferTimeAttributeDescriptor(persistedFeatureType);
  }

  @Override
  public synchronized TimeDescriptors getTimeDescriptors() {
    if (timeDescriptors == null) {
      timeDescriptors = TimeUtils.inferTimeAttributeDescriptor(persistedFeatureType);
    }
    return timeDescriptors;
  }

  @Override
  public HadoopWritableSerializer<SimpleFeature, FeatureWritable> createWritableSerializer() {
    return new FeatureWritableSerializer(reprojectedFeatureType);
  }

  private static class FeatureWritableSerializer implements
      HadoopWritableSerializer<SimpleFeature, FeatureWritable> {
    private final FeatureWritable writable;

    FeatureWritableSerializer(final SimpleFeatureType type) {
      writable = new FeatureWritable(type);
    }

    @Override
    public FeatureWritable toWritable(final SimpleFeature entry) {
      writable.setFeature(entry);
      return writable;
    }

    @Override
    public SimpleFeature fromWritable(final FeatureWritable writable) {
      return writable.getFeature();
    }
  }

  // this is not thread-safe, but should be ok given the only modification is on initialization
  // which is a synchronized operation
  private final transient BiMap<String, Integer> fieldToPositionMap = HashBiMap.create();
  private transient BiMap<Integer, String> positionToFieldMap = null;
  private final transient Map<String, List<String>> modelToDimensionsMap =
      new ConcurrentHashMap<>();
  private transient volatile boolean positionMapsInitialized = false;

  @Override
  public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
    int numDimensions;
    if (model != null) {
      final List<String> dimensionFieldNames = getDimensionFieldNames(model);
      // first check CommonIndexModel dimensions
      if (dimensionFieldNames.contains(fieldName)) {
        return dimensionFieldNames.indexOf(fieldName);
      }
      numDimensions = dimensionFieldNames.size();
    } else {
      numDimensions = 0;
    }
    if (!positionMapsInitialized) {
      synchronized (this) {
        initializePositionMaps();
      }
    }
    // next check other fields
    // dimension fields must be first, add padding
    final Integer position = fieldToPositionMap.get(fieldName);
    if (position == null) {
      return -1;
    }
    return position.intValue() + numDimensions;
  }

  @Override
  public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
    final List<String> dimensionFieldNames = getDimensionFieldNames(model);
    if (position >= dimensionFieldNames.size()) {
      final int adjustedPosition = position - dimensionFieldNames.size();
      if (!positionMapsInitialized) {
        synchronized (this) {
          initializePositionMaps();
        }
      }
      // check other fields
      return positionToFieldMap.get(adjustedPosition);
    }
    // otherwise check CommonIndexModel dimensions
    return dimensionFieldNames.get(position);
  }

  private void initializePositionMaps() {
    if (positionMapsInitialized) {
      return;
    }
    try {
      for (int i = 0; i < reprojectedFeatureType.getAttributeCount(); i++) {
        final AttributeDescriptor ad = reprojectedFeatureType.getDescriptor(i);
        final String currFieldName = ad.getLocalName();
        fieldToPositionMap.forcePut(currFieldName, i);
      }
      positionToFieldMap = fieldToPositionMap.inverse();
      positionMapsInitialized = true;
    } catch (final Exception e) {
      LOGGER.error("Unable to initialize position map, continuing anyways", e);
    }
  }

  protected List<String> getDimensionFieldNames(final CommonIndexModel model) {
    final List<String> retVal = modelToDimensionsMap.get(model.getId());
    if (retVal != null) {
      return retVal;
    }
    final List<String> dimensionFieldNames = DataStoreUtils.getUniqueDimensionFields(model);
    modelToDimensionsMap.put(model.getId(), dimensionFieldNames);
    return dimensionFieldNames;
  }

  public static CoordinateReferenceSystem decodeCRS(final String crsCode) {

    CoordinateReferenceSystem crs = null;
    try {
      crs = CRS.decode(crsCode, true);
    } catch (final FactoryException e) {
      LOGGER.error("Unable to decode '" + crsCode + "' CRS", e);
      throw new RuntimeException("Unable to initialize '" + crsCode + "' object", e);
    }

    return crs;
  }

  @Override
  public Map<String, String> describe() {
    Map<String, String> description = new HashMap<>();
    List<AttributeDescriptor> descriptors = this.persistedFeatureType.getAttributeDescriptors();
    for (AttributeDescriptor descriptor : descriptors) {
      description.put(descriptor.getLocalName(), descriptor.getType().getBinding().getName());
    }
    return description;
  }
}
