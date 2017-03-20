package weblog;

import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * Created by Gautam on 2017-03-19.
 */
public class LogLine implements Serializable {

    private DateTime dateTime;

    private String ipAddress;

    private String url;

    public DateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(DateTime dateTime) {
        this.dateTime = dateTime;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
