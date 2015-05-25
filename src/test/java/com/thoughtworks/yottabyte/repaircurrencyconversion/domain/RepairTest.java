package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import org.junit.Test;

import static com.natpryce.makeiteasy.MakeItEasy.*;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.Currency.RUPEES;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataMaker.cost;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataMaker.dummyDollarRepair;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RepairTest {


  @Test
  public void shouldBeAbleToConvertCurrencies() {
    Repair repairInDollars = new Repair(make((a(dummyDollarRepair,with(cost,10.0)))));
    Repair repairConvertedToRupees = repairInDollars.convertTo(RUPEES);
    assertThat(repairConvertedToRupees.getCurrency(),is(RUPEES));
    assertThat(repairConvertedToRupees.getAmount(),is(634.9));
  }
}
