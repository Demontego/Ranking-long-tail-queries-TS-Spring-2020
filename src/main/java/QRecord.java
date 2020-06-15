import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class QRecord implements Writable {
    private static String DELIMETER = "\t";
    private static String QUERY_GEO_DELIMETER = "@";
    private static String TIMES_DELIMETER = ",";

    public String query = "";
    public int queryGeo = -1;
    public List<String> shownLinks = new ArrayList<>();
    public List<String> clickedLinks = new ArrayList<>();
    public List<String> shownHosts = new ArrayList<>();
    public List<String> clickedHosts = new ArrayList<>();
    public List<Boolean> clickedPositions = new ArrayList<Boolean>();
    public List<Long> timestamps = new ArrayList<>();
    public Map<String, Integer> time_map = new HashMap<>();
    public boolean hasTimeMap;

    public QRecord() {
    }

    public QRecord(final String query, final int queryGeo, final List<String> shownLinks,
            final List<String> clickedLinks) {
        this.query = query;
        this.queryGeo = queryGeo;
        this.shownLinks = shownLinks;
        this.clickedLinks = clickedLinks;
    }

    public QRecord(final String query, final int queryGeo, final List<String> shownLinks,
            final List<String> clickedLinks, final List<Long> timestamps) {
        this.query = query;
        this.queryGeo = queryGeo;
        this.shownLinks = shownLinks;
        this.clickedLinks = clickedLinks;
        this.timestamps = timestamps;
    }

    public QRecord(final String data_string) throws URISyntaxException {
        this.parseString(data_string);
    }

    public void parseString(final String in) throws URISyntaxException {
        int idx = 0;
        final String[] args = in.split(DELIMETER);

        final String[] query_args = args[idx++].split(QUERY_GEO_DELIMETER);

        query = prepareQuery(query_args[0]);
        queryGeo = Integer.parseInt(query_args[1]);

        shownLinks = new ArrayList<>();
        String tmp = args[idx++].replace(",https://",",http://");
        tmp = tmp.replaceFirst("^https://", "http://");
        tmp = tmp.replaceAll(",(?!http://)([a-zA-Z]+)", ",http://$1");
        tmp = tmp.replaceFirst("^http://", "");
        String[] shownLinks_args = tmp.split(",http://");
        for ( String url : shownLinks_args) {  
            url = url.startsWith("wwww.")?url.substring(4):url;  
            shownLinks.add(url);
            shownHosts.add(prepareHost(url));
        }

        clickedLinks = new ArrayList<>();
        tmp =  args[idx++].replace(",https://",",http://");
        tmp = tmp.replaceFirst("^https://", "http://");
        tmp = tmp.replaceAll(",(?!http://)([a-zA-Z]+)", ",http://$1");
        tmp = tmp.replaceFirst("^http://", "");
        String[] clickedLinks_args = tmp.split(",http://");
        for (String url : clickedLinks_args) {
                url = url.startsWith("wwww.")?url.substring(4):url;
                clickedHosts.add(prepareHost(url));
                clickedLinks.add(url);
        }
        int k=0, m=0;
        while(k<shownLinks.size() && m< clickedLinks.size()){
            if(shownLinks.get(k).equals(clickedLinks.get(m))){
                clickedPositions.add(true);
                k++;m++;
            }else{
                clickedPositions.add(false);
                k++;
            }
        }
        for (int i=k; i<shownLinks.size(); ++i)
            clickedPositions.add(false);

        timestamps = new ArrayList<>();
        final String[] timestamps_args = args[idx++].split(TIMES_DELIMETER);
        for (final String timestamp : timestamps_args) {
            timestamps.add(Long.parseLong(timestamp));
        }
        if (timestamps.size() == clickedLinks.size()) {
            this.hasTimeMap = true;
            for (int i = 0; i < clickedLinks.size() - 1; ++i) {
                final Long time = (timestamps.get(i + 1) - timestamps.get(i)) / 1000;
                time_map.put(clickedLinks.get(i), time.intValue());
            }
            time_map.put(clickedLinks.get(clickedLinks.size() - 1), 30*60);
        }
        query = prepareQuery(query);
    }

    public static String prepareQuery(final String query) {
        return query.trim();
    }


    public static String prepareHost(final String url) throws URISyntaxException {
        String hurl =url;
        int ind = hurl.toString().indexOf("/");
        if (ind==-1)
            return hurl;
        String h =  hurl.substring(0,ind);
        return h;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeUTF(query);
        out.writeInt(queryGeo);

        out.writeInt(shownLinks.size());
        for (int i = 0; i < shownLinks.size(); i++) {
            out.writeUTF(shownLinks.get(i));
        }

        out.writeInt(clickedLinks.size());
        for (int i = 0; i < clickedLinks.size(); i++) {
            out.writeUTF(clickedLinks.get(i));
        }

        out.writeInt(timestamps.size());
        for (int i = 0; i < timestamps.size(); i++) {
            out.writeLong(timestamps.get(i));
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        query = in.readUTF();
        queryGeo = in.readInt();

        shownLinks = new ArrayList<>();
        final int shownLinksSize = in.readInt();
        for (int i = 0; i < shownLinksSize; i++) {
            shownLinks.add(in.readUTF());
        }

        clickedLinks = new ArrayList<>();
        final int clickedLinksSize = in.readInt();
        for (int i = 0; i < clickedLinksSize; i++) {
            clickedLinks.add(in.readUTF());
        }

        timestamps = new ArrayList<>();
        final int timestampsSize = in.readInt();
        for (int i =0; i< timestampsSize; i++)
        {
            timestamps.add(in.readLong());
        }
    }
}