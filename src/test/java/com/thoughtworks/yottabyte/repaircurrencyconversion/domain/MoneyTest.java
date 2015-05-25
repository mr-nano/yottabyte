package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import com.googlecode.zohhak.api.TestWith;
import com.googlecode.zohhak.api.runners.ZohhakRunner;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.Currency;
import org.junit.runner.RunWith;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.Currency.UNKNOWN;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(ZohhakRunner.class)
public class MoneyTest {

  @TestWith({
    "DOLLARS, 10, RUPEES, 634.90",
    "POUNDS, 34, DOLLARS, 52.292",
    "DOLLARS, 1, DOLLARS, 1",
  })
  public void shouldBeAbleToConvertFromAnother(Currency originalCurrency, Double originalAmount,
                                               Currency newCurrency, Double newExpectedAmount){
    Money originalMoney = new Money(originalCurrency,originalAmount);
    Money newMoney = originalMoney.convertTo(newCurrency);
    assertThat(newMoney.getCurrency(),is(newCurrency));
    assertThat(newMoney.getAmount(),is(newExpectedAmount));
  }

  @TestWith({
    "1, DOLLARS",
    "10, UNKNOWN",
  })
  public void shouldNotConvertIfItsOriginallyInUnknownCurrency(Double originalAmount, Currency newCurrency){
    Money originalMoney = new Money(UNKNOWN,originalAmount);
    Money newMoney = originalMoney.convertTo(newCurrency);
    assertThat(newMoney.getCurrency(),is(UNKNOWN));
    assertThat(newMoney.getAmount(),is(originalAmount));
  }

}
