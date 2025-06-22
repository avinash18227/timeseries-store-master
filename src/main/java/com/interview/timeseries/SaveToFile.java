package com.interview.timeseries;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SaveToFile implements SaveData{
    @Override
    public boolean writeToPersistentMemory(String fileName, HashMap<String,TreeMap<Long,List<DataPoint>>> dataPoints ) {
        try (ObjectOutputStream file = new ObjectOutputStream(new FileOutputStream(fileName))) {
            for(Map.Entry<String, TreeMap<Long, List<DataPoint>>> hashMap : dataPoints.entrySet()){
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

    @Override
    public boolean writeToMemeory(String fileName, HashMap<String, TreeMap<Long, List<DataPoint>>> dataPoints, TimeSeriesStore store) {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(fileName))) {
            while (true) {
                try {
                    DataPoint point = (DataPoint) in.readObject();
//                    System.out.println("writeToMemeory "+point);
                    store.insert(point.getTimestamp(), point.getMetric(),point.getValue(),point.getTags());
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
