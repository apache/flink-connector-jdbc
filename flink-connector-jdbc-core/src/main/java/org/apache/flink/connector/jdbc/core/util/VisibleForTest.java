package org.apache.flink.connector.jdbc.core.util;

import org.apache.flink.annotation.PublicEvolving;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * This annotations declares that a function, field, constructor, or entire type, is only visible
 * for testing purposes.
 *
 * <p>This annotation is typically attached when for example a method should be {@code private}
 * (because it is not intended to be called externally), but cannot be declared private, because
 * some tests need to have access to it.
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
@PublicEvolving
public @interface VisibleForTest {}
