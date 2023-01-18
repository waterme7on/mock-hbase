package org.waterme7on.hbase.procedure2;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Keep track of the runnable procedures
 */
public interface ProcedureScheduler {
    /**
     * Start the scheduler
     */
    void start();

    /**
     * Stop the scheduler
     */
    void stop();

    /**
     * In case the class is blocking on poll() waiting for items to be added, this
     * method should awake
     * poll() and poll() should return.
     */
    void signalAll();

    /**
     * Inserts the specified element at the front of this queue.
     * 
     * @param proc the Procedure to add
     */
    void addFront(Procedure proc);

    /**
     * Inserts the specified element at the front of this queue.
     * 
     * @param proc   the Procedure to add
     * @param notify whether need to notify worker
     */
    void addFront(Procedure proc, boolean notify);

    /**
     * Inserts all elements in the iterator at the front of this queue.
     */
    void addFront(Iterator<Procedure> procedureIterator);

    /**
     * Inserts the specified element at the end of this queue.
     * 
     * @param proc the Procedure to add
     */
    void addBack(Procedure proc);

    /**
     * Inserts the specified element at the end of this queue.
     * 
     * @param proc   the Procedure to add
     * @param notify whether need to notify worker
     */
    void addBack(Procedure proc, boolean notify);

    /**
     * The procedure can't run at the moment. add it back to the queue, giving
     * priority to someone
     * else.
     * 
     * @param proc the Procedure to add back to the list
     */
    void yield(Procedure proc);

    /**
     * The procedure in execution completed. This can be implemented to perform
     * cleanups.
     * 
     * @param proc the Procedure that completed the execution.
     */
    void completionCleanup(Procedure proc);

    /**
     * Returns true if there are procedures available to process, otherwise false.
     */
    boolean hasRunnables();

    /**
     * Fetch one Procedure from the queue
     * 
     * @return the Procedure to execute, or null if nothing present.
     */
    Procedure poll();

    /**
     * Fetch one Procedure from the queue
     * 
     * @param timeout how long to wait before giving up, in units of unit
     * @param unit    a TimeUnit determining how to interpret the timeout parameter
     * @return the Procedure to execute, or null if nothing present.
     */
    Procedure poll(long timeout, TimeUnit unit);

    /**
     * List lock queues.
     * 
     * @return the locks
     */
    List<LockedResource> getLocks();

    /**
     * @return {@link LockedResource} for resource of specified type & name. null if
     *         resource is not
     *         locked.
     */
    LockedResource getLockResource(LockedResourceType resourceType, String resourceName);

    /**
     * Returns the number of elements in this queue.
     * 
     * @return the number of elements in this queue.
     */
    int size();

    /**
     * Clear current state of scheduler such that it is equivalent to newly created
     * scheduler. Used
     * for testing failure and recovery. To emulate server crash/restart,
     * {@link ProcedureExecutor}
     * resets its own state and calls clear() on scheduler.
     */
    void clear();
}
