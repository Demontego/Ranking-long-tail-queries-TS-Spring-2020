import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;





public class CTR extends Configured implements Tool {

    static private Map<String, Integer> get_url(final Mapper.Context context, final Path pt) {
        final Map<String, Integer> map = new HashMap<>();
        try {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                final String s = split[1].charAt(split[1].length() - 1) == '/'
                            ? split[1].substring(0, split[1].length() - 1)
                            : split[1];
                map.put(s, Integer.parseInt(split[0]));

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
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                map.put(split[1].trim(), split[0].trim());
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    public static class CTRMapper extends Mapper<LongWritable, Text, Text, Text> {
        static final IntWritable one = new IntWritable(1);
        static final IntWritable null_0 = new IntWritable(0);
        static Map<String, Integer> ids= new HashMap<>();
        static Map<String, String> queries= new HashMap<>();

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            final Path urls = new Path("check/url.data/url.data");
            final Path q = new Path("check/fspell.txt");
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(urls), "UTF8"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                final String s = split[1].charAt(split[1].length() - 1) == '/'
                        ? split[1].substring(0, split[1].length() - 1)
                        : split[1];
                ids.put(s, Integer.parseInt(split[0]));

            }

            bufferedReader = new BufferedReader(new InputStreamReader(fs.open(q), "UTF8"));
            while ((line = bufferedReader.readLine()) != null) {
                final String[] split = line.split("\t");
                queries.put(split[1].trim(), split[0].trim());
            }
            
           

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
                    context.write(new Text(queries.get(record.query)), new Text(val));
                }
            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class CTRReducer extends Reducer<Text, Text, Text, Text> {

        
        @Override
        protected void reduce(final Text key, final Iterable<Text> nums, final Context context)
                throws IOException, InterruptedException {
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
                context.write(key,new Text(res));

            
           
    }
}

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());

        job.setJarByClass(PBM.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(CTRMapper.class);
        job.setReducerClass(CTRReducer.class);
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
        final int ret = ToolRunner.run(new CTR(), args);
        System.exit(ret);
    }
}
