package com.thoughtworks.yottabyte.constants;

public enum FileNameConstants {
  REPAIR_IN_DIFFERENT_CURRENCIES,
  REPAIR_IN_DOLLARS,
  VEHICLES,
  OLD_VEHICLES,
  VEHICLES_REPAIRS;

  public String columnSeparator(){
    return this.toString() + "." + MetaConstants.COLUMN_SEPARATOR;
  }

  public String path(){
    return this.toString() + "." + MetaConstants.PATH;
  }

  public String distributedCacheLocation(){
    return "./" + this.toString();
  }

  public String refrenceDate() {
    return this.toString() + "." + MetaConstants.REFERENCE_DATE;
  }

  public String refrenceDateFormat() {
    return this.toString() + "." + MetaConstants.REFERENCE_DATE_FORMAT;
  }
}
