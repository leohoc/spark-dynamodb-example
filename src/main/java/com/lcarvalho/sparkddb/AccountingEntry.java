package com.lcarvalho.sparkddb;

import java.math.BigDecimal;

public class AccountingEntry {

    private String debitAccount;
    private String creditAccount;
    private BigDecimal value;

    public AccountingEntry(String debitAccount, String creditAccount, BigDecimal value) {
        this.debitAccount = debitAccount;
        this.creditAccount = creditAccount;
        this.value = value;
    }

    public String getDebitAccount() {
        return debitAccount;
    }

    public void setDebitAccount(String debitAccount) {
        this.debitAccount = debitAccount;
    }

    public String getCreditAccount() {
        return creditAccount;
    }

    public void setCreditAccount(String creditAccount) {
        this.creditAccount = creditAccount;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }
}
