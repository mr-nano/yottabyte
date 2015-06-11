package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import org.junit.Test;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.Currency.RUPEES;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataBuilders.dummyDollarRepair;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RepairTest {

  @Test
  public void shouldBeAbleToConvertCurrencies() {
    Repair repairInDollars = new Repair(dummyDollarRepair().amount(10.0).build());
    Repair repairConvertedToRupees = repairInDollars.convertTo(RUPEES);
    assertThat(repairConvertedToRupees.getCurrency(),is(RUPEES));
    assertThat(repairConvertedToRupees.getAmount(),is(634.9));
  }
}
