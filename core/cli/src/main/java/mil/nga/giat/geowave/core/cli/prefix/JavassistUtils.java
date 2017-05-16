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
package mil.nga.giat.geowave.core.cli.prefix;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ConstPool;
import javassist.bytecode.Descriptor;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.MemberValue;

/**
 * These functions make it less of a pain to deal with Javassist. There's one to
 * find methods, and one to clone annotations, which is used in several places
 * within JCommanderPrefixTranslator.
 */
public class JavassistUtils
{

	private static Logger LOGGER = LoggerFactory.getLogger(JavassistUtils.class);

	public static final String PREFIX_PACKAGE = "mil.nga.giat.geowave.core.cli.parsed";

	private static final String uniqueId;
	private static int objectCounter = 0;

	static {
		uniqueId = UUID.randomUUID().toString().replace(
				'-',
				'_');
	}

	private JavassistUtils() {}

	/**
	 * This function will take the given annotations attribute and create a new
	 * attribute, cloning all the annotations and specified values within the
	 * attribute. The annotations attribute can then be set on a method, class,
	 * or field.
	 * 
	 * @param attr
	 * @return
	 */
	public static AnnotationsAttribute cloneAnnotationsAttribute(
			ConstPool constPool,
			AnnotationsAttribute attr,
			ElementType validElementType ) {

		// We can use system class loader here because the annotations for
		// Target
		// are part of the Java System.
		ClassLoader cl = ClassLoader.getSystemClassLoader();

		AnnotationsAttribute attrNew = new AnnotationsAttribute(
				constPool,
				AnnotationsAttribute.visibleTag);

		if (attr != null) {
			for (Annotation annotation : attr.getAnnotations()) {
				Annotation newAnnotation = new Annotation(
						annotation.getTypeName(),
						constPool);

				// If this must target a certain type of field, then ensure we
				// only
				// copy over annotations that can target that type of field.
				// For instances, a METHOD annotation can't be applied to a
				// FIELD or TYPE.
				Class<?> annoClass;
				try {
					annoClass = cl.loadClass(annotation.getTypeName());
					Target target = annoClass.getAnnotation(Target.class);
					if (target != null && !Arrays.asList(
							target.value()).contains(
							validElementType)) {
						continue;
					}
				}
				catch (ClassNotFoundException e) {
					// Cannot apply this annotation because its type cannot be
					// found.
					LOGGER.error(
							"Cannot apply this annotation because it's type cannot be found",
							e);
					continue;
				}

				// Copy over the options for this annotation. For example:
				// @Parameter(names = "-blah")
				// For this, a member value would be "names" which would be a
				// StringMemberValue
				if (annotation.getMemberNames() != null) {
					for (Object memberName : annotation.getMemberNames()) {
						MemberValue memberValue = annotation.getMemberValue((String) memberName);
						if (memberValue != null) {
							newAnnotation.addMemberValue(
									(String) memberName,
									memberValue);
						}
					}
				}
				attrNew.addAnnotation(newAnnotation);
			}
		}
		return attrNew;
	}

	/**
	 * This class will find the method in the CtClass, and return it as a
	 * CtMethod.
	 * 
	 * @param clz
	 * @param m
	 * @return
	 * @throws NotFoundException
	 */
	public static CtMethod findMethod(
			CtClass clz,
			Method m )
			throws NotFoundException {
		ClassPool pool = ClassPool.getDefault();
		Class<?>[] paramTypes = m.getParameterTypes();
		List<CtClass> paramTypesCtClass = new ArrayList<CtClass>();
		for (Class<?> claz : paramTypes) {
			paramTypesCtClass.add(pool.get(claz.getName()));
		}
		String desc = Descriptor.ofMethod(
				pool.get(m.getReturnType().getName()),
				paramTypesCtClass.toArray(new CtClass[] {}));
		CtMethod method = clz.getMethod(
				m.getName(),
				desc);
		return method;
	}

	/**
	 * Simple helper method to essentially clone the annotations from one class
	 * onto another
	 * 
	 * @param oldClass
	 * @param newClass
	 */
	public static void copyClassAnnotations(
			CtClass oldClass,
			CtClass newClass ) {
		// Load the existing annotations attributes
		AnnotationsAttribute classAnnotations = (AnnotationsAttribute) oldClass.getClassFile().getAttribute(
				AnnotationsAttribute.visibleTag);

		// Clone them
		AnnotationsAttribute copyClassAttribute = JavassistUtils.cloneAnnotationsAttribute(
				newClass.getClassFile2().getConstPool(),
				classAnnotations,
				ElementType.TYPE);

		// Set the annotations on the new class
		newClass.getClassFile().addAttribute(
				copyClassAttribute);
	}

	/**
	 * Simple helper method to take any FIELD targetable annotations from the
	 * method and copy them to the new field. All JCommander annotations can
	 * target fields as well as methods, so this should capture them all.
	 * 
	 * @param method
	 * @param field
	 */
	public static void copyMethodAnnotationsToField(
			CtMethod method,
			CtField field ) {
		// Load the existing annotations attributes
		AnnotationsAttribute methodAnnotations = (AnnotationsAttribute) method.getMethodInfo().getAttribute(
				AnnotationsAttribute.visibleTag);

		// Clone them
		AnnotationsAttribute copyMethodAttribute = JavassistUtils.cloneAnnotationsAttribute(
				field.getFieldInfo2().getConstPool(),
				methodAnnotations,
				ElementType.FIELD);

		// Set the annotations on the new class
		field.getFieldInfo().addAttribute(
				copyMethodAttribute);
	}

	/**
	 * Allows us to generate unique class names for generated classes
	 * 
	 * @return
	 */
	public static String getNextUniqueClassName() {
		return String.format(
				"%s.cli_%s_%d",
				PREFIX_PACKAGE,
				uniqueId,
				objectCounter++);
	}

	/**
	 * Allows us to generate unique field names for generated classes
	 * 
	 * @return
	 */
	public static String getNextUniqueFieldName() {
		return String.format(
				"field_%d",
				objectCounter++);
	}

	/**
	 * This will generate a class which is empty. Useful for applying
	 * annotations to it
	 * 
	 * @return
	 */
	public static CtClass generateEmptyClass() {
		// Create the class, so we can start adding the new facade fields to it.
		ClassPool pool = ClassPool.getDefault();
		return pool.makeClass(getNextUniqueClassName());
	}
}
