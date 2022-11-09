package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.concurrent.TimeUnit;

public class TimeAmount {
    private final int amount;
    private final TimeUnit timeUnit;

    private TimeAmount(int amount, TimeUnit timeUnit) {
        this.amount = amount;
        this.timeUnit = timeUnit;
    }

    public int getAmount() {
        return amount;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public String toString() {
        return "TimeAmount{ " + amount + " " + timeUnit + " }";
    }

    static TimeAmount parseTimeAmount(String timeAmount) {
        int sepPos = timeAmount.indexOf(' ');
        if (sepPos <= 0) {
            throw new IllegalArgumentException("Invalid time amount definition: " + timeAmount);
        }
        int value = Integer.parseInt(timeAmount.substring(0, sepPos).trim());
        TimeUnit timeUnit = parseTimeUnit(timeAmount.substring(sepPos + 1).trim());
        return new TimeAmount(value, timeUnit);
    }

    private static TimeUnit parseTimeUnit(String unit) {
        if (unit.equalsIgnoreCase("millisecond") || unit.equalsIgnoreCase("milliseconds")) {
            return TimeUnit.MILLISECONDS;
        }
        if (unit.equalsIgnoreCase("second") || unit.equalsIgnoreCase("seconds")) {
            return TimeUnit.SECONDS;
        }
        if (unit.equalsIgnoreCase("minute") || unit.equalsIgnoreCase("minutes")) {
            return TimeUnit.MINUTES;
        }
        if (unit.equalsIgnoreCase("hour") || unit.equalsIgnoreCase("hours")) {
            return TimeUnit.HOURS;
        }
        if (unit.equalsIgnoreCase("day") || unit.equalsIgnoreCase("days")) {
            return TimeUnit.DAYS;
        }
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
}
