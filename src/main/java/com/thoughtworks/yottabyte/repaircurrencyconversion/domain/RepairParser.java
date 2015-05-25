package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import com.thoughtworks.yottabyte.datamodels.RepairData;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RepairParser {

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

  public List<Repair> parse(File repairsFile) throws IOException {
    List<Repair> repairs = new ArrayList<>();
    String record;
    try(BufferedReader bufferedReader = new BufferedReader(new FileReader(repairsFile))){
      while ((record = bufferedReader.readLine()) != null){
        repairs.add(parse(record));
      }
    }
    return repairs;
  }
}
