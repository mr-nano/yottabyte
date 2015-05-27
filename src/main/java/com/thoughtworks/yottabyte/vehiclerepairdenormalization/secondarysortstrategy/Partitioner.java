package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.TaggedText;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<TaggedKey,TaggedText> {
  @Override
  public int getPartition(TaggedKey key, TaggedText taggedText, int numberOfReducers) {
    return key.getIdentifier().hashCode() % numberOfReducers;
  }
}
