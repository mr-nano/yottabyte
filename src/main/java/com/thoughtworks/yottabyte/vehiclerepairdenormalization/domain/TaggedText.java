package com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain;

import org.apache.hadoop.io.Text;

public class TaggedText<E extends Enum<E>> extends Text {

  private E tag;

  public TaggedText(Text row, E tag){
    this.tag = tag;
    set(row);
  }

  public E tag(){
    return tag;
  }

  public String row(){
    return this.toString().split("::")[0];
  }

}
