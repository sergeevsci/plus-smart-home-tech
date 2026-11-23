package ru.yandex.practicum.telemetry.collector.model;

public final class LightSensorEvent extends SensorEvent {
    private int linkQuality;
    private int luminosity;

    public int getLinkQuality() { return linkQuality; }
    public void setLinkQuality(int linkQuality) { this.linkQuality = linkQuality; }
    public int getLuminosity() { return luminosity; }
    public void setLuminosity(int luminosity) { this.luminosity = luminosity; }
}
