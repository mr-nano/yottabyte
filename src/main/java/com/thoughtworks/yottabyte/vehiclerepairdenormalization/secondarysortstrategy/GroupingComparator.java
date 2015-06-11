package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
  protected GroupingComparator() {
    super(TaggedKey.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {

    TaggedKey key1 = (TaggedKey) w1;
    TaggedKey key2 = (TaggedKey) w2;

    return key1.getVehicleType().compareTo(key2.getVehicleType());
  }
}