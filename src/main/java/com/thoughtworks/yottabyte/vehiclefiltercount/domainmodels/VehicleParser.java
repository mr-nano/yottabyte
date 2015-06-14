package com.thoughtworks.yottabyte.vehiclefiltercount.domainmodels;

import com.thoughtworks.yottabyte.FileParser;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import org.apache.hadoop.io.Text;

public class VehicleParser extends FileParser<Vehicle> {
  private String columnSeparator;
  private String dateTimeFormat;

  public VehicleParser(String columnSeparator, String dateTimeFormat) {
    this.columnSeparator = columnSeparator;
    this.dateTimeFormat = dateTimeFormat;
  }

  public Vehicle parse(Text record) {
    return new Vehicle(new VehicleData(record.toString(),columnSeparator,dateTimeFormat));
  }

  @Override
  public Vehicle parse(String record){
    return parse(new Text(record));
  }

}
