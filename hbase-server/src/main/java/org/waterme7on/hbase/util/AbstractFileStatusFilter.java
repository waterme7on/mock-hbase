package org.waterme7on.hbase.util;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Typical base class for file status filter. Works more efficiently when
 * filtering file statuses,
 * otherwise implementation will need to lookup filestatus for the path which
 * will be expensive.
 */
public abstract class AbstractFileStatusFilter implements PathFilter {

    /**
     * Filters out a path. Can be given an optional directory hint to avoid
     * filestatus lookup.
     * 
     * @param p     A filesystem path
     * @param isDir An optional boolean indicating whether the path is a directory
     *              or not
     * @return true if the path is accepted, false if the path is filtered out
     */
    protected abstract boolean accept(Path p, Boolean isDir);

    public boolean accept(FileStatus f) {
        return accept(f.getPath(), f.isDirectory());
    }

    @Override
    public boolean accept(Path p) {
        return accept(p, null);
    }

    protected boolean isFile(FileSystem fs, Boolean isDir, Path p) throws IOException {
        return !isDirectory(fs, isDir, p);
    }

    protected boolean isDirectory(FileSystem fs, Boolean isDir, Path p)
            throws IOException {
        return isDir != null ? isDir : fs.isDirectory(p);
    }
}
