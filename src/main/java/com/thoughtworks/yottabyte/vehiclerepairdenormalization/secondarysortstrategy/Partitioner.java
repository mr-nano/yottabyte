package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import org.apache.hadoop.io.Text;

import static java.lang.Math.abs;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<TaggedKey,Text> {
  @Override
  public int getPartition(TaggedKey key, Text taggedText, int numberOfReducers) {
    return abs(key.getVehicleType().hashCode()) % numberOfReducers;
  }
}
