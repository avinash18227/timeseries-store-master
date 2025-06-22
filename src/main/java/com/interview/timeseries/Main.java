package com.interview.timeseries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Simple main class for manual testing of the TimeSeriesStore implementation.
 */
public class Main {
    public static void main(String[] args) {
        // Create and initialize the store
//        ReadWriteLock lock = new ReentrantReadWriteLock();
        TimeSeriesStore store = new TimeSeriesStoreImpl();
        boolean initialized = store.initialize();
        System.out.println("Store initialized: " + initialized);
        
        if (!initialized) {
            System.err.println("Failed to initialize the store. Exiting.");
            return;
        }
        
        try {
            // Insert some test data
            long now = System.currentTimeMillis();
            Map<String, String> tags = new HashMap<>();
            tags.put("host", "server1");
            tags.put("datacenter", "us-west");
            
            boolean inserted = store.insert(now, "cpu.usage", 45.2, tags);
            inserted = store.insert(now, "cpu.usage", 46.2, tags);
            System.out.println("Data inserted: " + inserted);
            
            // Query the data
            List<DataPoint> results = store.query("cpu.usage", now - 1000, now + 1000, null);
            System.out.println("Query results: " + results);

            results = store.query("cpu.usage", 1750588783541l - 1000, 1750588783541l + 1000, null);
            System.out.println("Old query results: " + results);
            
            // Query with filters
            Map<String, String> filters = new HashMap<>();
            filters.put("host", "server1");
//            filters.put("host1", "server2");
            results = store.query("cpu.usage", now - 1000, now + 1000, filters);
            System.out.println("Filtered query results: " + results);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // Shut down the store
            boolean shutdown = store.shutdown();
            System.out.println("Store shutdown: " + shutdown);
        }
    }
}
