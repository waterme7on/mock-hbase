package org.waterme7on.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;

public interface OnlineRegions {

    /**
     * Add to online regions.
     */
    void addRegion(final HRegion r);

    /**
     * Removes the given Region from the list of onlineRegions.
     * 
     * @param r           Region to remove.
     * @param destination Destination, if any, null otherwise.
     * @return True if we removed a region from online list.
     */
    boolean removeRegion(final HRegion r, ServerName destination);

    /**
     * Return {@link Region} instance. Only works if caller is in same context, in
     * same JVM. Region is
     * not serializable.
     * 
     * @return Region for the passed encoded <code>encodedRegionName</code> or null
     *         if named region is
     *         not member of the online regions.
     */
    Region getRegion(String encodedRegionName);

    /**
     * Get all online regions of a table in this RS.
     * 
     * @return List of Region
     * @throws java.io.IOException
     */
    List<? extends Region> getRegions(TableName tableName) throws IOException;

    /**
     * Get all online regions in this RS.
     * 
     * @return List of online Region
     */
    List<? extends Region> getRegions();
}
