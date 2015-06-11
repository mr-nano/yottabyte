package com.thoughtworks.yottabyte.constants;

import static com.thoughtworks.yottabyte.constants.MetaConstants.*;

public enum FileNameConstants {
  REPAIR_IN_DIFFERENT_CURRENCIES,
  REPAIR_IN_DOLLARS,
  REPAIRS,
  VEHICLES,
  OLD_VEHICLES,
  VEHICLES_REPAIRS;

  public String columnSeparator() {
    return this.toString() + "." + COLUMN_SEPARATOR;
  }

  public String path() {
    return this.toString() + "." + PATH;
  }

  public String distributedCacheLocation() {
    return "./" + this.toString();
  }

  public String referenceDate() {
    return this.toString() + "." + REFERENCE_DATE;
  }

  public String referenceDateFormat() {
    return this.toString() + "." + REFERENCE_DATE_FORMAT;
  }

  public String dateFormat() {
    return this.toString() + "." + DATE_FORMAT;
  }

}
