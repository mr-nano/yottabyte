package com.thoughtworks.yottabyte.datamodels;

import com.google.common.base.Joiner;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RepairData {
  private String vehicleType;
  private String code;
  private String description;
  private String currency;
  private Double amount;

  public RepairData(String row, String parsingRegex) {
    String[] columns = row.split(parsingRegex);
    assignColumns(columns);
  }

  protected void assignColumns(String[] columns) {
    vehicleType = columns[0];
    code = columns[1];
    description = columns[2];
    currency = columns[3].trim().toUpperCase();
    amount = Double.valueOf(columns[4]);
  }

  public String toStringRepresentation(String separator) {
    return Joiner.on(separator)
      .join(new Object[]{
        vehicleType, code, description,currency, amount
      });
  }
}