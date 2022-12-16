package org.waterme7on.hbase;

import org.apache.hadoop.conf.Configuration;

public interface Server extends org.apache.hadoop.hbase.Abortable, org.apache.hadoop.hbase.Stoppable {

    Configuration getConfiguration();
}
