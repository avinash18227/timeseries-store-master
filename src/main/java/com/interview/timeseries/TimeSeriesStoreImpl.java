package com.interview.timeseries;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of the TimeSeriesStore interface.
 * 
 * TODO: This is a skeleton implementation that needs to be completed.
 */
public class TimeSeriesStoreImpl implements TimeSeriesStore {
    
    // TODO: Define appropriate data structures for storing time series data
    private final HashMap<String,TreeMap<Long,List<DataPoint>>> dataPoints = new HashMap<>();
    private final static ReadWriteLock lock = new ReentrantReadWriteLock();
//    public TimeSeriesStoreImpl(ReadWriteLock lock){
//        this.lock = lock;
//    }

    // TODO: Define thread safety mechanism
    
    // TODO: Define persistence mechanism
    private String fileName = "fileTimeSeries.ser";

    @Override
    public boolean insert(long timestamp, String metric, double value, Map<String, String> tags) {
        // TODO: Implement the insert operation
        try {
            lock.writeLock().lock();
            if (!dataPoints.containsKey(metric)) {
                dataPoints.put(metric, new TreeMap<>());
            }
            TreeMap<Long, List<DataPoint>> treeMap = dataPoints.get(metric);
//            treeMap.put(timestamp, new DataPoint(timestamp, metric, value, tags));
            treeMap.computeIfAbsent(timestamp,t ->new ArrayList<>()).add(new DataPoint(timestamp, metric, value, tags));
        } catch (Exception e) {
            return false;
        } finally {
            lock.writeLock().unlock();
        }
        return true; // Placeholder
    }
    
    @Override
    public List<DataPoint> query(String metric, long timeStart, long timeEnd, Map<String, String> filters) {
        // TODO: Implement the query operation
        try {
            lock.readLock().lock();
            if (dataPoints.containsKey(metric)) {
                TreeMap<Long, List<DataPoint>> treeMap = dataPoints.get(metric);
                NavigableMap<Long, List<DataPoint>> sub = treeMap.subMap(timeStart, true, timeEnd, false);
                return filter(sub, filters);
            }
        }finally {
            lock.readLock().unlock();
        }
        return new ArrayList<>(); // Placeholder
    }
    
    @Override
    public boolean initialize() {
        // TODO: Implement initialization
        return writeToMemeory(); // Placeholder
    }
    
    @Override
    public boolean shutdown() {
        // TODO: Implement shutdown
        return writeToFile(); // Placeholder
    }
    
    // TODO: Add helper methods as needed
    private List<DataPoint> filter(NavigableMap<Long,List<DataPoint>> sub, Map<String, String> filters){
        List<DataPoint> list = new ArrayList<>();
        for(Map.Entry<Long,List<DataPoint>> dataPoint : sub.entrySet()){
            List<DataPoint> arr = dataPoint.getValue();
            for(DataPoint d : arr){
                boolean add = true;
                if(filters != null) {
                    for (Map.Entry<String, String> filter : filters.entrySet()) {
                        if (!d.getTags().containsKey(filter.getKey()) ||
                                !d.getTags().get(filter.getKey()).equals(filter.getValue())) {
                            add = false;
                            break;
                        }
                    }
                }
                if(add){
                    list.add(d);
                }
            }
        }
        return list;
    }
    private boolean writeToFile(){
        try (ObjectOutputStream file = new ObjectOutputStream(new FileOutputStream(fileName))) {
            for(Map.Entry<String,TreeMap<Long,List<DataPoint>>> hashMap : dataPoints.entrySet()){
                for(Map.Entry<Long,List<DataPoint>> treeMap : hashMap.getValue().entrySet()){
                    for(DataPoint point : treeMap.getValue()){
                        file.writeObject(point);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    private boolean writeToMemeory(){
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(fileName))) {
            while (true) {
                try {
                    DataPoint point = (DataPoint) in.readObject();
//                    System.out.println("writeToMemeory "+point);
                    insert(point.getTimestamp(), point.getMetric(),point.getValue(),point.getTags());
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
