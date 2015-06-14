package com.thoughtworks.yottabyte.vehiclefiltercount;

import org.apache.hadoop.io.Text;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<Text,Text> {
  @Override
  public int getPartition(Text key, Text value, int numPartitions) {

    switch (key.toString().toUpperCase()){
      case "CAR" : return 0;
      case "TRUCK" : return 1;
      case "RIKSHAW" : return 2;
      case "BUS" : return 3;
      default: return 0;
    }

  }
}


