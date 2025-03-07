package fr.yaon.utils;

import java.io.Serializable;

public class TemperatureData implements Serializable {
    double temperatureSum;
    int temperatureCount;

    public TemperatureData() {
        this.temperatureSum = 0.0;
        this.temperatureCount = 0;
    }

    private TemperatureData(double temperatureSum, int temperatureCount) {
        this.temperatureSum = temperatureSum;
        this.temperatureCount = temperatureCount;
    }

    public double getTemperatureSum() {
        return temperatureSum;
    }

    public void setTemperatureSum(double temperatureSum) {
        this.temperatureSum = temperatureSum;
    }

    public int getTemperatureCount() {
        return temperatureCount;
    }

    public void setTemperatureCount(int temperatureCount) {
        this.temperatureCount = temperatureCount;
    }

    public TemperatureData addTemperature(double temperature) {
        return new TemperatureData(this.temperatureSum + temperature, this.temperatureCount + 1);
    }

    public double computeAverageTemperature() {
        return this.temperatureSum / this.temperatureCount;
    }
}
