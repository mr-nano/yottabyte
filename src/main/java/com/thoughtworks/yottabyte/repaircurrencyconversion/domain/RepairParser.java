package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import com.thoughtworks.yottabyte.FileParser;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import org.apache.hadoop.io.Text;

public class RepairParser extends FileParser<Repair> {

  private String columnSeparator;

  public RepairParser(String columnSeparator) {
    this.columnSeparator = columnSeparator;
  }

  public Repair parse(Text record) {
    return new Repair(new RepairData(record.toString(),columnSeparator));
  }

  public Repair parse(String record){
    return parse(new Text(record));
  }

}
