package hadoopApp;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

import java.io.IOException;
import java.io.IOException;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class ProcessUnit {


    //Mapper class
    public static class E_EMapper extends MapReduceBase implements
            Mapper<LongWritable,/*Input key Type */
                    Text,                /*Input value Type*/
                    Text,                /*Output key Type*/
                    IntWritable>        /*Output value Type*/ {

        //Map function
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String line = value.toString();
            String[] words = line.split(",");
            for (String word : words) {
                Text outputKey = new Text(word.toUpperCase().trim());
                IntWritable outputValue = new IntWritable(1);
                output.collect(outputKey, outputValue);
            }
        }
    }


    //Reducer class
    public static class E_EReduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        //Reduce function
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            for (Iterator<IntWritable> it = values; it.hasNext(); ) {
                IntWritable value = it.next();
                sum += value.get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }


    //Main function
    public static void main(String args[]) throws Exception {


        Scanner reader = new Scanner(System.in);  // Reading from System.in
        System.out.println("Enter a number: ");
        int n = reader.nextInt(); // Scans the next token of the input as an int.
        //once finished
        System.out.println("you've enetered: " + n);
        reader.close();


        FileSystem hdfs = FileSystem.get(new Configuration());
        Path homeDir = hdfs.getHomeDirectory();
//Print the home directory

       // System.out.println("Home folder -" + homeDir);

        Path path = new Path(args[1]);

        JobConf conf = new JobConf(ProcessUnit.class);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(path);

        conf.setJobName("max_eletricityunits");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(E_EMapper.class);
        conf.setCombinerClass(E_EReduce.class);
        conf.setReducerClass(E_EReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);

        Path workingDir=hdfs.getWorkingDirectory();
        Path newFolderPath= new Path(workingDir,"MyDataFolder");
        //newFolderPath=Path.mergePaths(workingDir, newFolderPath);
        Path newFilePath=new Path(newFolderPath+"/newFile.txt");
        if(hdfs.exists(newFilePath)){
            System.out.println("already existing");
            hdfs.delete(newFilePath,true);
        }else{
            System.out.println("dont know");
        }

        if(hdfs.exists(newFilePath)){
            System.out.println("already existing");
            hdfs.delete(newFilePath,true);
        }else{
            System.out.println("dont know");
        }

        if(hdfs.exists(newFolderPath))
        {
            hdfs.delete(newFolderPath, true); //Delete existing Directory
        }

        hdfs.mkdirs(newFolderPath);     //Create new Directory

        System.out.println("Home folder -" + homeDir);
        System.out.println("working dir -" + workingDir);

        reader = new Scanner(System.in);  // Reading from System.in
        System.out.println("Enter a number: ");
        n = reader.nextInt(); // Scans the next token of the input as an int.
        //once finished
        System.out.println("you've enetered: " + n);
        reader.close();



        hdfs.createNewFile(newFilePath);
        StringBuilder sb=new StringBuilder();for(int i=1;i<=5;i++)
        {
            sb.append("data");
            sb.append(i);
            sb.append("\n");
        }

        byte[] byt=sb.toString().getBytes();

        FSDataOutputStream fsOutStream = hdfs.create(newFilePath);

        fsOutStream.write(byt);
        fsOutStream.close();


        BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
        String str = null;
        String finalString="";
        while ((str = bfr.readLine())!= null)

        {
            finalString = finalString + str;
           //System.out.println(str);

        }

        System.out.println(finalString);
        try {
            path = new Path(args[2]);
            FileSystem fileSystem = FileSystem.get(new Configuration());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
            String line = bufferedReader.readLine();
            while (line != null) {
                System.out.println(line);
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*try
        {
            String verify, putData;
            FileSystem fileSystem = FileSystem.get(new Configuration());

            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.open(path)));

            File file = new File("/Dairy.txt");
            file.createNewFile();
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("I am Shah Khalid");
            bw.flush();

            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);

            while( (verify=br.readLine()) != null )
            {
                if(verify != null)
                {
                    System.out.println(verify);
                }
            }
            br.close();
            bw.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }*/


        System.out.println(": complete :");

        JobConf confTwo = new JobConf(ProcessUnit.class);
        System.out.println("Second job starts :\n = " + args[0] + ", " + args[1] + ", " + args[2] + "\n");

        confTwo.setJobName("max_eletricityunits");
        confTwo.setOutputKeyClass(Text.class);
        confTwo.setOutputValueClass(IntWritable.class);
        confTwo.setMapperClass(E_EMapper.class);
        confTwo.setCombinerClass(E_EReduce.class);
        confTwo.setReducerClass(E_EReduce.class);
        confTwo.setInputFormat(TextInputFormat.class);
        confTwo.setOutputFormat(TextOutputFormat.class);
        fs = FileSystem.get(confTwo);
        path = new Path(args[1]);
        fs.delete(path);
        FileInputFormat.setInputPaths(confTwo, new Path(args[0]/*+"/ss.txt"*/));
        FileOutputFormat.setOutputPath(confTwo, new Path(args[1]));
        JobClient.runJob(confTwo);
    }
}