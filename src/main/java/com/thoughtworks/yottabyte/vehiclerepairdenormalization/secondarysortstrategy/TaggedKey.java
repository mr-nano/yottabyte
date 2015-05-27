package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.RecordType;
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
  private String identifier;

  private RecordType tag;

  public TaggedKey(String identifier, RecordType tag) {
    this.identifier = identifier;
    this.tag = tag;
  }

  @Override
  public int compareTo(TaggedKey o) {
    return tag.compareTo(o.tag);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeString(out,identifier);
    writeEnum(out, tag);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    identifier = readString(in);
    tag = readEnum(in,RecordType.class);
  }
}
