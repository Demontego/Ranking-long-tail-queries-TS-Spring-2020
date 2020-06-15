import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class PBM extends Configured implements Tool{

    public static final String DOC = "DOCS";
    public static final String QDOC = "QDOCS";
    public static final String HOST = "HOST";
    public static final String QHOST = "QHOST";
    public static final String QUERY = "QUERY";
    
    static private Map<String, String> get_url(final Mapper.Context context, final Path pt) {
        final Map<String, String> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt),"UTF8"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                String s = split[1].charAt(split[1].length() - 1) == '/' ? split[1].substring(0, split[1].length() - 1)
                        : split[1];
                s = s.startsWith("www.") ? s.substring(4) : s;
                map.put(s, split[0]);
                }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    static private Map<String, String> get_host(final Mapper.Context context, final Path pt) {
        final Map<String, String> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt),"UTF8"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                int ind = split[1].toString().indexOf("/");
                String h =  split[1].substring(0,ind);
                h = h.startsWith("www.") ? h.substring(4) : h;
                map.put(h, split[0]);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    static private Map<String, String> get_queries(final Mapper.Context context, final Path pt) {
        final Map<String, String> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt), "UTF8"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                map.put(split[1].trim(), split[0]);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    public static class PBMMapper extends Mapper<LongWritable, Text, Text, Text> {

        static Map<String, String> ids;
        static Map<String, String> hosts;
        static Map<String, String> queries;
        static Map<String, String> queries_spell;

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            final Path urls = new Path("check/url.data/url.data");
            final Path q = new Path("check/queries.tsv");
            final Path q_spell = new Path("check/fspell.txt");
            ids = get_url(context, urls);
            queries = get_queries(context, q);
            queries_spell =  get_queries(context, q_spell);
            hosts = get_host(context, urls);

        }

        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            try {
                final QRecord record = new QRecord();//клик|первый|последний|место показа|позиция клика|время|показ в условиии каскада|был ли клик на предыдущий документ|был ли клик на следующий документ|расстояние до ближайшей кликнутой ссылку сверху|снизу
                record.parseString(value.toString());
                boolean flag_query = true;
                String qid = queries.get(record.query);
                if (qid==null){
                    qid= queries_spell.get(record.query);
                    if (qid==null)
                        flag_query=false;
                }
                if(flag_query){
                    int[] res = new int[6];//количество показанных ссылок|количество  кликнутых ссылок|время работы с запросом|позиция первой кликнутой ссылки|средняя позиция документов|были ли клики
                    res[0] = record.shownLinks.size();
                    res[1] = record.clickedLinks.size();
                    if(record.hasTimeMap){
                        int time = 0;
                        for(int i=0; i< record.clickedLinks.size();++i){
                            time+=record.time_map.get(record.clickedLinks.get(i)); 
                        }
                        res[2]=time;
                    }
                    int avg = 0;
                    for(int i=0; i<record.clickedLinks.size();++i)
                        avg+=record.shownLinks.indexOf(record.clickedLinks.get(i))+1;
                    if(record.clickedLinks.size()!=0){
                        res[3]=record.shownLinks.indexOf(record.clickedLinks.get(0))+1;
                        res[4] = avg / record.clickedLinks.size();
                    }else{
                        res[5]=1;
                    }
                    String val = String.valueOf(res[0])+'|'+String.valueOf(res[1])+'|'+String.valueOf(res[2])+'|'+String.valueOf(res[3])+'|'+
                    String.valueOf(res[4])+'|'+String.valueOf(res[5]);
                    context.write(new Text("QUERY|"+qid), new Text(val));
                }
                for (int i = 0; i < record.shownLinks.size(); i++) {
                        if (ids.containsKey(record.shownLinks.get(i))) {
                            int[] res = new int[11];
                            if (record.clickedPositions.get(i)){
                                res[0]=1;
                                if(record.clickedLinks.indexOf(record.shownLinks.get(i))==0)
                                    res[1]=1;
                                if (record.clickedLinks.indexOf(record.shownLinks.get(i))==record.clickedLinks.size()-1)
                                    res[2]=1;
                                res[4]=record.clickedLinks.indexOf(record.shownLinks.get(i))+1;
                                if(record.hasTimeMap)
                                    res[5]=record.time_map.get(record.shownLinks.get(i));
                                int k=0,m=i-1;
                                while(m>0 && !record.clickedPositions.get(m)){--m;++k;}
                                res[9]= m>=0 ? k: 0;
                                k=0; m=i+1;
                                while(m<record.clickedPositions.size() && !record.clickedPositions.get(m)){++m;++k;}
                                res[10]=m!=record.clickedPositions.size() ? k : 0;
                            }
                            if(record.clickedLinks.size()==0)
                                res[6]=1;
                            for(int j= 0; j < record.clickedLinks.size();++j){
                                if(record.shownLinks.indexOf(record.clickedLinks.get(j))<i)
                                    break;
                                if(record.shownLinks.indexOf(record.clickedLinks.get(j))>i){
                                    res[6]=1;
                                    break;
                                }
                                if(i!=0)
                                    res[7]=record.clickedPositions.get(i-1) ? 1 : 0;
                                if(i!=record.shownLinks.size()-1)
                                    res[8] =record.clickedPositions.get(i+1) ? 1 : 0;
                            }
                            res[3]=i+1;
                            String val=String.valueOf(res[0])+"|"+String.valueOf(res[1])+"|"+String.valueOf(res[2])+"|"+
                                String.valueOf(res[3])+"|"+String.valueOf(res[4])+"|"+String.valueOf(res[5])+"|"+String.valueOf(res[6])+
                                "|"+String.valueOf(res[7])+"|"+String.valueOf(res[8])+"|"+String.valueOf(res[9])+"|"+String.valueOf(res[10]);                            
                            if(flag_query){
                                context.write(new Text("QDOC|"+qid+'\t'+String.valueOf(ids.get(record.shownLinks.get(i)))),new Text(val));
                            }
                            context.write(new Text("DOC|"+String.valueOf(ids.get(record.shownLinks.get(i)))),new Text(val));
                        }
                    }
                    for (int i = 0; i < record.shownHosts.size(); i++) {
                        if (hosts.containsKey(record.shownHosts.get(i))) {
                            int[] res = new int[11];
                            if (record.clickedPositions.get(i)){
                                res[0]=1;
                                if(record.clickedHosts.indexOf(record.shownHosts.get(i))==0)
                                    res[1]=1;
                                if (record.clickedHosts.indexOf(record.shownHosts.get(i))==record.clickedHosts.size()-1)
                                    res[2]=1;
                                res[4]=record.clickedHosts.indexOf(record.shownHosts.get(i))+1;
                                if(record.hasTimeMap){
                                    res[5]=record.time_map.get(record.shownLinks.get(i));
                                int k=0,m=i-1;
                                while(m>0 && !record.clickedPositions.get(m)){--m;++k;}
                                res[9]=k;
                                k=0; m=i+1;
                                while(m<record.clickedPositions.size() && !record.clickedPositions.get(m)){++m;++k;}
                                res[10]= k;
                                }
                            }
                            if(record.clickedHosts.size()==0)
                                res[6]=1;
                            for(int j= 0; j < record.clickedLinks.size();++j){
                                if(record.shownHosts.indexOf(record.clickedHosts.get(j))<i)
                                    break;
                                if(record.shownHosts.indexOf(record.clickedHosts.get(j))>i){
                                    res[6]=1;
                                    break;
                                }
                                if(i>0)
                                    res[7]=record.clickedPositions.get(i-1) ? 1 : 0;
                                if(i<record.shownLinks.size()-1)
                                    res[8] =record.clickedPositions.get(i+1) ? 1 : 0;
                            }
                            res[3]=i+1;
                            String val=String.valueOf(res[0])+"|"+String.valueOf(res[1])+"|"+String.valueOf(res[2])+"|"+
                                String.valueOf(res[3])+"|"+String.valueOf(res[4])+"|"+String.valueOf(res[5])+"|"+String.valueOf(res[6])+
                                "|"+String.valueOf(res[7])+"|"+String.valueOf(res[8])+"|"+String.valueOf(res[9])+"|"+String.valueOf(res[10]);
                            if(flag_query){
                                context.write(new Text("QHOST|"+qid+'\t'+String.valueOf(hosts.get(record.shownHosts.get(i)))),new Text(val));
                            } 
                            context.write(new Text("HOST|"+String.valueOf(hosts.get(record.shownHosts.get(i)))),new Text(val));
                        }
                    }
            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class PBMReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> multipleOutputs;

        public void setup(final Reducer.Context context) {
            multipleOutputs = new MultipleOutputs(context);

        }

        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
            String[] keys = key.toString().split("\\|");
            if (keys[0].equals("QUERY")){
                int count_q= 0;
                int shows_q= 0;
                int clicks_q= 0;
                int time = 0;
                int first_click_q = 0;
                int avg_pos_click= 0;
                int shows_noclick_q = 0;
                for(Text val : nums){
                    String[] data = val.toString().split("\\|");
                    count_q+=1;
                    shows_q+=Integer.valueOf(data[0]);
                    clicks_q+=Integer.valueOf(data[1]);
                    time+=Integer.valueOf(data[2]);
                    first_click_q+=Integer.valueOf(data[3]);
                    avg_pos_click+=Integer.valueOf(data[4]);
                    shows_noclick_q+=Integer.valueOf(data[5]);
                }
                String res = "count_query:"+String.valueOf(count_q)+ "\tshows_docs:" + String.valueOf(shows_q)+"\tclicks_docs:"+String.valueOf(clicks_q)+
                "\ttime:"+String.valueOf(time)+"\tfirst_click:"+String.valueOf(first_click_q)+"\tavg_pos_click:"+String.valueOf(avg_pos_click)+
                "\tshows_noclick:"+String.valueOf(shows_noclick_q);
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), QUERY+"/part");
            }else{
            int[] click_pos= new int[12];
            int[] shows_pos= new int[12];
            int[] show_pos_if_click = new int[12];
            int clicks = 0;
            int shows= 0;
            double log_time = 0.;
            int cm_shows = 0;
            int first_click = 0;
            int last_click= 0;
            int cm_clicks = 0;
            int upclick = 0;
            int downclick = 0;
            int showbeforeclick=0;
            int showafterclick = 0;
            for (final Text val : nums) {
                String[] data = val.toString().split("\\|");
                int click = Integer.parseInt(data[0]);
                int pos_show = Integer.parseInt(data[3]);
                int pos_click = Integer.parseInt(data[4]);
                int fclick = Integer.parseInt(data[1]);
                int lckick = Integer.parseInt(data[2]);
                log_time+=Math.log1p(Integer.parseInt(data[5]));
                cm_shows += Integer.parseInt(data[6]);
                upclick += Integer.parseInt(data[7]);
                downclick += Integer.parseInt(data[8]);
                showbeforeclick+=Integer.parseInt(data[9]);;
                showafterclick = Integer.parseInt(data[10]);;
                if(Integer.parseInt(data[6])!=0)
                    cm_clicks+=click;
                shows++;
                clicks+=click;
                first_click+=fclick;
                last_click+=lckick;
                if(pos_show<11){
                    shows_pos[pos_show]+=1;
                    show_pos_if_click[pos_show]+=click;
                }
                else{    
                    shows_pos[11]+=1;
                    show_pos_if_click[11]+=click;
                }
                if(pos_click<11)
                    click_pos[pos_click]+=1;
                else
                    click_pos[11]+=1;
            }
            String res = "shows:"+String.valueOf(shows)+"\tclicks:"+String.valueOf(clicks)+"\ttime:"+String.valueOf(log_time)+"\tfirst_click:"+String.valueOf(first_click)+
                "\tlast_click:"+String.valueOf(last_click)+"\tcm_shows:"+String.valueOf(cm_shows)+"\tcm_clicks:"+String.valueOf(cm_clicks)+
                "\tup_click:"+String.valueOf(upclick)+"\tdown_click:"+String.valueOf(downclick)+"\tbefore_click:"+String.valueOf(showbeforeclick)
                +"\tafter_click:"+String.valueOf(showafterclick)+"\tclick_pos:";
            for(int i=1;i<12;++i){
                res+=String.valueOf(click_pos[i])+" ";
            }
            res+="\tshows_pos:";
            for(int i=1;i<12;++i){
                res+=String.valueOf(shows_pos[i])+" ";
            }
            res+="\tshow_pos_if_click:";
            for(int i=1;i<12;++i){
                res+=String.valueOf(show_pos_if_click[i])+" ";
            }
            switch(keys[0]){
            case("DOC"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), DOC+"/part");
                break;
            case("QDOC"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), QDOC+"/part");
                break;
            case("HOST"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), HOST+"/part");
                break;
            case("QHOST"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), QHOST+"/part");
                break;
            default:
                break;
            }
        }
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());

        job.setJarByClass(PBM.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, DOC, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QDOC, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, HOST, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QHOST, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY, TextOutputFormat.class, Text.class, Text.class);

        job.setMapperClass(PBMMapper.class);
        job.setReducerClass(PBMReducer.class);
        job.setNumReduceTasks(1);
        return job;
    }

    @Override
    public int run(final String[] args) throws Exception {
        
        final FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        final Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new PBM(), args);
        System.exit(ret);
    }
}