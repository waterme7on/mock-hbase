package org.waterme7on.hbase.ipc;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.apache.hadoop.hbase.HConstants;

/**
 * Annotation which decorates RPC methods to denote the relative priority among
 * other RPCs in the
 * same server. Provides a basic notion of quality of service (QOS).
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface QosPriority {
    int priority() default HConstants.NORMAL_QOS;
}
