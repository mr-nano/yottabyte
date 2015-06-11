package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableUtils.*;

@NoArgsConstructor
public class TaggedKey implements WritableComparable<TaggedKey> {

  @Getter
  private String vehicleType;

  private Tag tag;

  public TaggedKey(String vehicleType, Tag tag) {
    this.vehicleType = vehicleType;
    this.tag = tag;
  }

  @Override
  public int compareTo(TaggedKey o) {
    return tag.compareTo(o.tag);
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
