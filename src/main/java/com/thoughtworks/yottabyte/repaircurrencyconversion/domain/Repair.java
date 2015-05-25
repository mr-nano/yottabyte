package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.Currency;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.CurrencyParser;
import lombok.Delegate;

public class Repair {

  @Delegate(types = RepairDataDelegations.class)
  private RepairData repairData;
  @Delegate(types = MoneyDelegations.class)
  private Money money;

  public Repair(RepairData repairData) {
    this.repairData = repairData;
    money = new Money(CurrencyParser.fromString(repairData.getCurrency()),
      repairData.getAmount());
  }

  public Repair convertTo(Currency to) {
    money = money.convertTo(to);
    repairData.setAmount(money.getAmount());
    repairData.setCurrency(money.getCurrency().toString());
    return this;
  }

  private interface RepairDataDelegations {
    String getVehicleType();
    String getCode();
    String getDescription();
    String toStringRepresentation(String separator);
  }

  private interface MoneyDelegations {
    Double getAmount();
    Currency getCurrency();
  }
}

