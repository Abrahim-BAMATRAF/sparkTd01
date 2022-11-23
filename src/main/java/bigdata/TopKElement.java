package bigdata;

import java.util.Objects;

public class TopKElement implements Comparable<TopKElement>, Cloneable {

    private String cityName;
    private int cityPopulation;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopKElement that = (TopKElement) o;
        return cityPopulation == that.cityPopulation && cityName.equals(that.cityName);
    }

    @Override
    public String toString() {
        return "TopKElement{" +
                "cityName='" + cityName + '\'' +
                ", cityPopulation=" + cityPopulation +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(cityName, cityPopulation);
    }

    public TopKElement(String cityName, int cityPopulation) {
        this.cityName = cityName;
        this.cityPopulation = cityPopulation;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public int getCityPopulation() {
        return cityPopulation;
    }

    public void setCityPopulation(int cityPopulation) {
        this.cityPopulation = cityPopulation;
    }

    @Override
    public int compareTo(TopKElement o) {
        return Integer.compare(this.cityPopulation, o.getCityPopulation());
    }

    @Override
    public TopKElement clone() {
        try {
            TopKElement clone = (TopKElement) super.clone();
            clone.setCityPopulation(this.cityPopulation);
            clone.setCityName(this.cityName);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
