package hadoopApp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Scanner;


public class FinalProject {

    public static int degreeOfTree = 0;
    public static JobConf jobConf;

    public static FileSystem hdfs;
    public static Path workingDir;
    public static Path  newFolderPath;
    public static String currentSize = "0";

    public static String root = "0";
    public static String currNode = "0";
    public static String parent = "0";
    public static String next = "0";

    public static boolean dNodeInserted = false;
    public static String lowLine;
    public static String highLine;
    public static String finalPointerToNextPage;
    public static boolean finalPointerToNextPageCalculated = false;
    public static boolean isDuplicateKey = false;

    public static String dataToBeInserted;
    public static String dataKeyToBeInserted;


    public static class MapperToInsertInLeafNode extends MapReduceBase implements
            Mapper<LongWritable,/*Input key Type */
                    Text,                /*Input value Type*/
                    Text,                /*Output key Type*/
                    IntWritable>        /*Output value Type*/ {

        //Map function
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            int curr=0;
            String line = value.toString();
            String[] array = line.split(",");
            if( (!finalPointerToNextPageCalculated) && (!(array[0].equals("-1") && array[0].equals("0"))) && Integer.parseInt(array[0])<Integer.parseInt(highLine) && Integer.parseInt(array[0])>=Integer.parseInt(lowLine) ){
                int data  = Integer.parseInt(array[2]);
                if(Integer.parseInt(dataKeyToBeInserted)>data && Integer.parseInt(lowLine)<Integer.parseInt(array[0])){
                    lowLine = array[0];
                }else if(Integer.parseInt(dataKeyToBeInserted)<data  && Integer.parseInt(highLine)<Integer.parseInt(array[0])){
                    highLine = array[0];
                }else if(Integer.parseInt(dataKeyToBeInserted)==data){
                    System.out.println("This data is already present in data base...\nError: Duplicate key..");
                    isDuplicateKey = true;
                }
            }
        }
    }

 /* int curr=0;
            String line = value.toString();
            String[] array = line.split(",");
            if( (!finalPointerToNextPageCalculated) && (!(array[0].equals("-1") && array[0].equals("0"))) && Integer.parseInt(array[0])<Integer.parseInt(highLine) && Integer.parseInt(array[0])>=Integer.parseInt(lowLine) ){
                int data  = Integer.parseInt(array[2]);
                if(curr>data){
                    lowLine = array[1];
                }else if(curr<data){
                    highLine = array[1];
                }else{
                    finalPointerToNextPage = array[2];
                    finalPointerToNextPageCalculated = true;
                }
            }*/

    //Reducer class
    public static class ReducerToInsertInLeafNode extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        //Reduce function
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        }
    }


    //Main function
    public static void main(String args[]) throws Exception {
        //first programme will start and welcome the user.....
        workingDir=hdfs.getWorkingDirectory();

        System.out.println("Entering into datavase.\nHello!!......................\n");

        System.out.println("Enter degree of tree:\n");
        Scanner reader = new Scanner(System.in);  // Reading from System.in
        degreeOfTree = reader.nextInt(); // Scans the next token of the input as an int.
        //once finished
        System.out.println("you've enetered: " + degreeOfTree);
        reader.close();
        insertIntoTree();

    }


    private static void insertIntoTree( ) throws IOException {

        if(root.equals("0")){
            createTree();
        }
        FileSystem hdfs = FileSystem.get(jobConf);
        Path newFilePath=new Path(newFolderPath+"/" +root+".txt");
        BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
        String str = null;
        String finalString="";
        str = bfr.readLine();
        System.out.println(finalString);
        String[] values = str.split(",");

        if(values[1].equals("1")){
           currentSize =  values[2];
           parent = values[3];
           next = values[3];
           insertIntoLeafNode();
        }else{

            insertIntoNonLeafNode();
        }

    }

    private static void insertIntoNonLeafNode(int currentSize, int parent) {

    }

    private static void insertIntoLeafNode() throws IOException {
        if(Integer.parseInt(currentSize) < degreeOfTree-1) {

            dNodeInserted = false;
            lowLine="0";
            highLine="0";
            finalPointerToNextPageCalculated =false;
            finalPointerToNextPage = "0";
            isDuplicateKey = false;
            JobConf conf = new JobConf(ProcessUnit.class);
            conf.setJobName("Inserting data into leaf node");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setMapperClass(FinalProject.MapperToInsertInLeafNode.class);
            conf.setCombinerClass(FinalProject.ReducerToInsertInLeafNode.class);
            conf.setReducerClass(FinalProject.ReducerToInsertInLeafNode.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            /*FileSystem fs = FileSystem.get(conf);
            path = new Path(args[1]);
            fs.delete(path);*/
            Path p = new Path("new.txt");
            if (hdfs.exists(p)){hdfs.delete(p,true);}
            Path path = new Path(newFolderPath+"/"+currNode+".txt");
            FileInputFormat.setInputPaths(conf, path);
            FileOutputFormat.setOutputPath(conf, p);
            JobClient.runJob(conf);


            String finalFile = "";
            try {

                FileSystem fileSystem = FileSystem.get(new Configuration());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                String line = bufferedReader.readLine();
               for (int i=0;i<Integer.parseInt(lowLine);i++){
                    finalFile = finalFile + line + "\n";
                    line = bufferedReader.readLine();
                }
                int lineNo = Integer.parseInt(lowLine) + 1;
                finalFile = finalFile + ""+lineNo + "," +dataToBeInserted + "\n";

                for (int i = Integer.parseInt(highLine); i<Integer.parseInt(currentSize); i++){
                    finalFile = finalFile + line + "\n";
                    line = bufferedReader.readLine();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            hdfs.delete(path,true);
            hdfs.createNewFile(path);
            StringBuilder sb=new StringBuilder();
            sb.append(finalFile);
            byte[] byt=sb.toString().getBytes();
            FSDataOutputStream fsOutStream = hdfs.create(path);
            fsOutStream.write(byt);
            fsOutStream.close();

        }
        // if the leaf is full split
        else {
            this.split(dnode);
        }

        // return the root of the tree
        return this.findRoot();
    }

    private static void createTree() throws IOException {
        jobConf = new JobConf(FinalProject.class);
        workingDir=hdfs.getWorkingDirectory();
        newFolderPath= new Path(workingDir,"Tree");
        if(hdfs.exists(newFolderPath)){hdfs.delete(newFolderPath, true);}
        hdfs.mkdirs(newFolderPath);
        Path newFilePath=new Path(newFolderPath+"/1.txt");
        if(hdfs.exists(newFilePath)){System.out.println("File already existed and deleted...");hdfs.delete(newFilePath,true);}
        StringBuilder sb=new StringBuilder();
        sb.append("-1,1," + degreeOfTree + ",0"+","+ "0," +"0"+ "\n");
        byte[] byt=sb.toString().getBytes();
        FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
        fsOutStream.write(byt);
        fsOutStream.close();
        System.out.println("Root node created.");
        root = "1";
        currNode = "1";
    }

}