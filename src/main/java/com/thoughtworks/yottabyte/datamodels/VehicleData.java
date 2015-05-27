package com.thoughtworks.yottabyte.datamodels;

import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class VehicleData {

  private String type;
  private String registrationNumber;
  private String owner;
  private DateTime date;

  public VehicleData(String row, String parsingRegex, String dateTimeFormat) {
    String[] columns = row.split(parsingRegex);
    assignColumns(columns,dateTimeFormat);
  }

  protected void assignColumns(String[] columns, String dateTimeFormat) {
    type = columns[0];
    registrationNumber = columns[1];
    owner = columns[2];
    date = DateTimeFormat.forPattern(dateTimeFormat)
      .parseDateTime(columns[3]);
  }

  public String toStringRepresentation(String separator) {
    return Joiner.on(separator)
      .join(new Object[]{
        type, registrationNumber, owner, date
      });
  }
}