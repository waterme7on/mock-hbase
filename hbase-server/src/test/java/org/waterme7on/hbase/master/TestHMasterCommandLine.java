package org.waterme7on.hbase.master;

import static org.junit.Assert.*;

import org.waterme7on.hbase.HBaseCommonTestingUtility;
import org.junit.Test;

public class TestHMasterCommandLine {

  private static final HBaseCommonTestingUtility TESTING_UTIL = new HBaseCommonTestingUtility();

  @Test
  public void testRun() throws Exception {
    HMasterCommandLine masterCommandLine = new HMasterCommandLine(HMaster.class);
    masterCommandLine.setConf(TESTING_UTIL.getConfiguration());
    assertEquals(1, masterCommandLine.run(new String[] { "clear" }));
  }
}
