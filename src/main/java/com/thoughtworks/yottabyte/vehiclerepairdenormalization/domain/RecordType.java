package com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain;

public enum RecordType {
  REPAIR,
  VEHICLE;

  public boolean isNot(RecordType type){
    return !this.equals(type);
  }
}

