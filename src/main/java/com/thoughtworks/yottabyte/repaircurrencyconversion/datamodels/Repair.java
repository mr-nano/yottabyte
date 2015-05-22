package com.thoughtworks.yottabyte.repaircurrencyconversion.datamodels;

import lombok.Getter;

@Getter
public class Repair {
  private String vehicleType;
  private String code;
  private String description;
  private Double cost;

  public Repair(String row, String parsingRegex){
    String[] columns = row.split(parsingRegex);
    assignColumns(columns);
  }

  protected void assignColumns(String[] columns) {
    vehicleType = columns[0];
    code = columns[1];
    description = columns[2];
    cost = Double.valueOf(columns[3]);
  }
}
