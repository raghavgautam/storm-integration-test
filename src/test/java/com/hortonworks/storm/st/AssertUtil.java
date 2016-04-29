package com.hortonworks.storm.st;

import org.testng.Assert;

import java.io.File;
import java.util.Collection;

/**
 * Created by temp on 4/28/16.
 */
public class AssertUtil {

    public static void empty(Collection<?> collection) {
        Assert.assertTrue(collection == null || collection.size() == 0, "Expected collection to be non-null, found: " + collection);
    }

    public static void nonEmpty(Collection<?> collection) {
        Assert.assertNotNull(collection, "Expected collection to be non-null, found: " + collection);
        greater(collection.size(), 0, "Expected collection to be non-empty, found: " + collection);
    }

    public static void greater(int actual, int expected, String message) {
        Assert.assertTrue(actual > expected, message);
    }

    public static void exists(File path) {
        Assert.assertNotNull(path, "Supplied path was expected to be non null, found: " + path);
        Assert.assertTrue(path.exists(), "Supplied path was expected to be non null, found: " + path);
    }
}
