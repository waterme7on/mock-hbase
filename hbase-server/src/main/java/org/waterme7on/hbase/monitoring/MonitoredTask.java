package org.waterme7on.hbase.monitoring;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface MonitoredTask extends Cloneable {
  enum State {
    RUNNING,
    WAITING,
    COMPLETE,
    ABORTED;
  }

  public interface StatusJournalEntry {
    String getStatus();

    long getTimeStamp();
  }

  long getStartTime();

  String getDescription();

  String getStatus();

  long getStatusTime();

  State getState();

  long getStateTime();

  long getCompletionTimestamp();

  long getWarnTime();

  void markComplete(String msg);

  void pause(String msg);

  void resume(String msg);

  void abort(String msg);

  void expireNow();

  void setStatus(String status);

  void setDescription(String description);

  void setWarnTime(final long t);

  /**
   * If journal is enabled, we will store all statuses that have been set along
   * with the time stamps
   * when they were set. This method will give you all the journals stored so far.
   */
  List<StatusJournalEntry> getStatusJournal();

  String prettyPrintJournal();

  /**
   * Explicitly mark this status as able to be cleaned up, even though it might
   * not be complete.
   */
  void cleanup();

  /**
   * Public exposure of Object.clone() in order to allow clients to easily capture
   * current state.
   * 
   * @return a copy of the object whose references will not change
   */
  MonitoredTask clone();

  /**
   * Creates a string map of internal details for extensible exposure of monitored
   * tasks.
   * 
   * @return A Map containing information for this task.
   */
  Map<String, Object> toMap() throws IOException;

  /**
   * Creates a JSON object for parseable exposure of monitored tasks.
   * 
   * @return An encoded JSON object containing information for this task.
   */
  String toJSON() throws IOException;

}
