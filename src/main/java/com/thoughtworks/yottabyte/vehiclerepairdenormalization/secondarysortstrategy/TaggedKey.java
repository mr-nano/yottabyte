package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.google.common.collect.ComparisonChain;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableUtils.*;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TaggedKey implements WritableComparable<TaggedKey> {

  private String vehicleType;
  private Tag tag;

  @Override
  public int compareTo(TaggedKey o) {
    return ComparisonChain.start()
      .compare(this.vehicleType,o.getVehicleType())
      .compare(this.getTag(),o.getTag())
      .result();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeString(out, vehicleType);
    writeEnum(out, tag);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vehicleType = readString(in);
    tag = readEnum(in,Tag.class);
  }
}
