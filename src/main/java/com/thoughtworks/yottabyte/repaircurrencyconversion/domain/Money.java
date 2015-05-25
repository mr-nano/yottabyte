package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.Currency;
import lombok.Getter;

@Getter
public class Money {
  private final Currency currency;
  private final Double amount;

  public Money(Currency currency, Double amount) {
    this.currency = currency;
    this.amount = amount;
  }

  public Money convertTo(Currency to) {
    if(eitherCurrenciesAreUnknown(currency,to)){
      return this;
    }
    return new Money(to,amount*getCurrency().getConversionFactor(to));
  }

  private boolean eitherCurrenciesAreUnknown(Currency one, Currency two){
    return one.isUnknown() || two.isUnknown();
  }
}
