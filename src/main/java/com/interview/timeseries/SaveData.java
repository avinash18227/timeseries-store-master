package com.interview.timeseries;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public interface SaveData {
    public boolean writeToPersistentMemory(String fileName, HashMap<String, TreeMap<Long, List<DataPoint>>> dataPoints);
    public boolean writeToMemeory(String fileName, HashMap<String, TreeMap<Long, List<DataPoint>>> dataPoints, TimeSeriesStore store);
}
