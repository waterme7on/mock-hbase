package org.waterme7on.hbase.master;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLInputFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.StringInterner;
import org.codehaus.stax2.XMLStreamReader2;
import org.waterme7on.hbase.HBaseCommonTestingUtility;

import com.ctc.wstx.api.ReaderConfig;
import com.ctc.wstx.io.StreamBootstrapper;
import com.ctc.wstx.io.SystemId;
import com.ctc.wstx.stax.WstxInputFactory;

import org.junit.Test;

public class TestHMasterCommandLine {

  private static final HBaseCommonTestingUtility TESTING_UTIL = new HBaseCommonTestingUtility();

  @Test
  public void testRun() throws Exception {
    HMasterCommandLine masterCommandLine = new HMasterCommandLine(HMaster.class);
    masterCommandLine.setConf(TESTING_UTIL.getConfiguration());
    assertEquals(1, masterCommandLine.run(new String[] { "clear" }));
  }

  @Test
  public void testConfiguration() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Path rootDir = new Path(System.getProperty("user.dir")).getParent();
    conf.addResource(new Path(rootDir, "conf/hbase-site.xml"));
    assertEquals("172.17.0.2", conf.get("hbase.zookeeper.quorum"));
  }
}
