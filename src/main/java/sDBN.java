import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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




public class sDBN extends Configured implements Tool {
    public static final String DOC = "DOCS";
    public static final String QDOC = "QDOCS";
    public static final String HOST = "HOST";
    public static final String QHOST = "QHOST";

    
    static private Map<String, String> get_url(final Mapper.Context context, final Path pt) {
        final Map<String, String> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt),"UTF8"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                final String s = split[1].charAt(split[1].length() - 1) == '/'
                            ? split[1].substring(0, split[1].length() - 1)
                            : split[1];
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

    public static class sDBNMapper extends Mapper<LongWritable, Text, Text, Text> {
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
                final QRecord record = new QRecord();
                record.parseString(value.toString());
                boolean flag_query = true;
                String qid = queries.get(record.query);
                if (qid==null){
                    qid= queries_spell.get(record.query);
                    if (qid==null)
                        flag_query=false;
                }
                for (int i = 0; i < record.shownLinks.size(); i++) {
                    if (ids.containsKey(record.shownLinks.get(i))) {
                        String[] flags=new String[2]; flags[0]="0"; flags[1]="0";
                        if(record.clickedPositions.get(i)){
                            flags[1]="1";
                            if(record.clickedLinks.indexOf(record.shownLinks.get(i))==record.clickedLinks.size()-1)
                                flags[1]="1";
                            String res = flags[0]+"|"+flags[1];
                            if(flag_query)
                                context.write(new Text("QDOC|"+qid+"\t"+String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                            context.write(new Text("DOC|"+String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                        }else{
                            if(i<record.shownLinks.indexOf(record.clickedLinks.get(record.clickedLinks.size()-1))){
                                String res = "0|0";
                                if(flag_query)
                                    context.write(new Text("QDOC|"+qid+"\t"+String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                                context.write(new Text("DOC|"+String.valueOf(ids.get(record.shownLinks.get(i)))), new Text(res));
                            }
                        }  
                    }
                }
                for (int i = 0; i < record.shownHosts.size(); i++) {
                    if (hosts.containsKey(record.shownHosts.get(i))) {
                        String[] flags=new String[2]; flags[0]="0"; flags[1]="0";
                        if(record.clickedPositions.get(i)){
                            flags[1]="1";
                            if(record.clickedHosts.indexOf(record.shownHosts.get(i))==record.clickedHosts.size()-1)
                                flags[1]="1";
                            String res = flags[0]+"|"+flags[1];
                            if(flag_query)
                                context.write(new Text("QHOST|"+qid+"\t"+String.valueOf(hosts.get(record.shownHosts.get(i)))), new Text(res));
                            context.write(new Text("HOST|"+String.valueOf(hosts.get(record.shownHosts.get(i)))), new Text(res));
                        }else{
                            if(i<record.shownHosts.indexOf(record.clickedHosts.get(record.clickedHosts.size()-1))){
                                String res = "0|0";
                                if(flag_query)
                                    context.write(new Text("QHOST|"+qid+"\t"+String.valueOf(hosts.get(record.shownHosts.get(i)))), new Text(res));
                                context.write(new Text("HOST|"+String.valueOf(hosts.get(record.shownHosts.get(i)))), new Text(res));
                            }
                        }  
                    }
                }
            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class sDBNReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> multipleOutputs;

        public void setup(final Reducer.Context context) {
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
            
            int s_d = 0;
            int s_n = 0;
            int a_d = 0;
            int a_n = 0;
            for(Text val:nums){
                String[] split = val.toString().split("\\|");
                s_n += Integer.valueOf(split[0]);
                a_n += Integer.valueOf(split[1]);
                s_d += Integer.valueOf(split[1]);
                a_d++;
            }
            Double A = (double)(a_n+0.1)/(a_d+0.1+0.1);
            Double S = (double)(s_n+0.1)/(s_d+0.1+0.1);
            Double R= A*S;
            String res = String.valueOf(A)+"\t"+String.valueOf(S)+'\t'+String.valueOf(R);
            String[] keys = key.toString().split("\\|");
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

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());

        job.setJarByClass(sDBN.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        MultipleOutputs.addNamedOutput(job, DOC, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QDOC, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, HOST, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QHOST, TextOutputFormat.class, Text.class, Text.class);

        job.setMapperClass(sDBNMapper.class);
        job.setReducerClass(sDBNReducer.class);
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
        final int ret = ToolRunner.run(new sDBN(), args);
        System.exit(ret);
    }
}