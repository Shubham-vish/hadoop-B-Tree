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
    public static Path newFolderPath;
    public static String currentSize = "0";

    public static String root = "0";
    public static String currNode = "0";
    public static String parent = "0";
    public static String next = "0";

    public static String dataToBeInserted;
    public static String dataKeyToBeInserted;

    public static void main(String args[]) throws Exception {

        //first programme will start and welcome the user.....
        jobConf = new JobConf(FinalProject.class);
        hdfs = FileSystem.get(jobConf);
        workingDir = hdfs.getWorkingDirectory();
        System.out.println("Entering into datavase.\nHello!!......................\n");
        System.out.println("Enter degree of tree:\n");
        Scanner reader = new Scanner(System.in);
        degreeOfTree = reader.nextInt();
        System.out.println("you've enetered: " + degreeOfTree);
        reader.close();
        insertIntoTree();
    }

    private static void insertIntoTree() throws IOException {
        if (root.equals("0")) {
            createTree();
        }
        hdfs = FileSystem.get(jobConf);
        Path newFilePath = new Path(newFolderPath + "/" + root + ".txt");
        BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
        String str;
        str = bfr.readLine();
        String[] values = str.split(",");
        currNode = root;
        currentSize = values[3];
        parent = values[4];
        next = values[5];
        if (values[1].equals("1")) {
            insertIntoLeafNode(bfr);
        } else {
            insertIntoNonLeafNode();
        }
    }

    private static void insertIntoNonLeafNode() {

    }

    private static void insertIntoLeafNode(BufferedReader bfr) throws IOException {
        if (Integer.parseInt(currentSize) < degreeOfTree - 1) {
            String finalFile = "";
            int currSize = Integer.parseInt(currentSize) + 1;
            finalFile = finalFile + -1 + ",1," + degreeOfTree + currSize + "," + parent + "," + next + "\n";
            String line = bfr.readLine();
            String[] v = line.split(",");
            int data = Integer.parseInt(v[1]);
            int i;
            for (i = 0; i < currSize && data < Integer.parseInt(dataKeyToBeInserted); i++) {
                finalFile = finalFile + line + "\n";
                line = bfr.readLine();
                v = line.split(",");
                data = Integer.parseInt(v[1]);
            }
            if (i >= currSize) {

            } else {
                finalFile = finalFile + i + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
                i++;
                for (; i < currSize; i++) {
                    v = line.split(",");
                    finalFile = finalFile + i + "," + v[1] + "," + v[2] + "\n";
                    line = bfr.readLine();
                }
            }
            Path newFilePath = new Path(newFolderPath + "/" + currNode + ".txt");
            hdfs.delete(newFilePath, true);
            hdfs.createNewFile(newFilePath);
            StringBuilder sb = new StringBuilder();
            sb.append(finalFile);
            byte[] byt = sb.toString().getBytes();
            FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
            fsOutStream.write(byt);
            fsOutStream.close();
        } else {
            splitLeaf(bfr);
        }
    }

    private static void splitLeaf(BufferedReader bfr) throws IOException {
        int midData;
        int splitlocation;
        if ((degreeOfTree - 1) % 2 == 0) {
            splitlocation = (degreeOfTree - 1) / 2;
        } else {
            splitlocation = ((degreeOfTree - 1) + 1) / 2;
        }
        Path path;
        int sizeOfSecondTree = 0;
        sizeOfSecondTree = Integer.parseInt(FinalProject.currentSize) - splitlocation;
        String finalFile = "";
        String secondfFile = "-1,1," + degreeOfTree + "," + sizeOfSecondTree + "," + FinalProject.parent + FinalProject.next + "\n";
        if (!next.equals("0")) {
            path = new Path(newFolderPath + "/" + FinalProject.next + "left.txt");
            finalFile = "" + -1 + "," + 1 + "," + degreeOfTree + splitlocation + "," + parent + "," + FinalProject.next + "left" + "\n";
            next = FinalProject.next + "left";
        } else {
            finalFile = "" + -1 + "," + 1 + "," + degreeOfTree + splitlocation + "," + parent + "," + FinalProject.currNode + "right" + "\n";
            path = new Path(newFolderPath + "/" + FinalProject.currNode + "right.txt");
            next = FinalProject.currNode + "right";
        }
        String line = bfr.readLine();
        String[] v = line.split(",");
        int data = Integer.parseInt(v[1]);
        int i;
        for (i = 0; i < splitlocation && line != null && data < Integer.parseInt(dataKeyToBeInserted); i++) {
            finalFile = finalFile + line + "\n";
            line = bfr.readLine();
            v = line.split(",");
            data = Integer.parseInt(v[1]);
        }
        if (i < splitlocation) {
            finalFile = finalFile + i + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
            i++;
            for (; i < splitlocation; i++) {
                v = line.split(",");
                finalFile = finalFile + i + "," + v[1] + "," + v[2] + "\n";
                line = bfr.readLine();
            }
            v = line.split(",");
            midData = Integer.parseInt(v[1]);
            for (; i < Integer.parseInt(currentSize) + 1; i++) {
                int lineNo = i - splitlocation;
                v = line.split(",");
                secondfFile = secondfFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                line = bfr.readLine();
            }
        } else {
            v = line.split(",");
            midData = Integer.parseInt(v[1]);
            for (; (i < Integer.parseInt(currentSize) + 1) && data < Integer.parseInt(dataKeyToBeInserted); i++) {
                int lineNo = i - splitlocation;
                v = line.split(",");
                secondfFile = secondfFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                line = bfr.readLine();
            }
            if (i < Integer.parseInt(currentSize) + 1) {
                secondfFile = secondfFile + i + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
                i++;
                for (; i < Integer.parseInt(currentSize) + 1; i++) {
                    int lineNo = i - splitlocation;
                    secondfFile = secondfFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                    line = bfr.readLine();
                }
            } else {
                secondfFile = secondfFile + i + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
            }
        }

        Path thisFile = new Path(newFolderPath + "/" + currNode + ".txt");
        hdfs.delete(thisFile, true);
        hdfs.createNewFile(thisFile);
        StringBuilder sb = new StringBuilder();
        sb.append(finalFile);
        byte[] byt = sb.toString().getBytes();
        FSDataOutputStream fsOutStream = hdfs.create(thisFile);
        fsOutStream.write(byt);
        fsOutStream.close();

        hdfs.delete(path, true);
        hdfs.createNewFile(path);
        sb = new StringBuilder();
        sb.append(secondfFile);
        byte[] bbyt = sb.toString().getBytes();
        FSDataOutputStream fssOutStream = hdfs.create(path);
        fssOutStream.write(bbyt);
        fssOutStream.close();

        propogate(String.valueOf(midData));
    }

    private static void propogate(String midData) throws IOException {

        if (parent.equals("0")) {//if parent is not present....create a new tree node and set it as root
            Path path = new Path(newFolderPath + "/" + currNode + "parent.txt");
            root = currNode + "parent";
            parent = "0";
            currNode = root;
            String string = "-1,0," + degreeOfTree + ",1,0" + "\n";//TODO:check here..
            string = string + "0" + "," + currNode + "\n";
            string = string + "1," + midData + "," + next + "\n";
            hdfs.delete(path, true);
            hdfs.createNewFile(path);
            StringBuilder sb = new StringBuilder();
            sb.append(string);
            byte[] bbyt = sb.toString().getBytes();
            FSDataOutputStream fssOutStream = hdfs.create(path);
            fssOutStream.write(bbyt);
            fssOutStream.close();
        } else {
            Path path = new Path(newFolderPath + "/" + parent + ".txt");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            String line = bufferedReader.readLine();
            String[] v = line.split(",");
            int sizeOfParent = Integer.parseInt(v[3]);
            currNode = parent;
            parent = v[4];
            if (sizeOfParent < degreeOfTree-1) {
                String finalFile = "";
                int newSize =  sizeOfParent +1;
                finalFile = "-1,0,"+degreeOfTree +","+newSize+","+parent + "\n";
                line = bufferedReader.readLine();
                finalFile = finalFile + line +"\n";//zeroth line is copied as it is...
                line = bufferedReader.readLine();
                int i;
                for (i = 1; i <= sizeOfParent && line != null && Integer.parseInt(midData) > Integer.parseInt(v[1]); i++) {
                    finalFile = finalFile + line + "\n";
                    line = bufferedReader.readLine();
                    v = line.split(",");
                }
                if(i <= sizeOfParent){
                    finalFile = finalFile + i + "," + midData + "," + next + "\n";
                    i++;
                    for (; i <= sizeOfParent && line != null; i++) {
                        v = line.split(",");
                        finalFile = finalFile + i + "," + v[1] + "," + v[2] + "\n";
                        line = bufferedReader.readLine();
                    }
                }else if(i==sizeOfParent+1){
                    finalFile = finalFile + i + "," + midData + "," + next + "\n";
                }
                hdfs.delete(path, true);
                hdfs.createNewFile(path);
                StringBuilder sb = new StringBuilder();
                sb.append(finalFile);
                byte[] bbyt = sb.toString().getBytes();
                FSDataOutputStream fssOutStream = hdfs.create(path);
                fssOutStream.write(bbyt);
                fssOutStream.close();
                currNode = parent;
            } else {
               splitTreeNode(midData,line,bufferedReader,v);
            }
        }
    }

    private static void splitTreeNode(String midData, String line, BufferedReader bufferedReader, String[] v) throws IOException {

        String finalFile = "",secondFile = "";
        String newMidData = "";
        String firstPointerForSecondFile;
        int splitlocation;
        if ((degreeOfTree - 1) % 2 == 0) {
            splitlocation = (degreeOfTree - 1) / 2;
        } else {
            splitlocation = ((degreeOfTree - 1) + 1) / 2;
        }
        //TODO:cross check here....
        int sizeOfSecondTree = Integer.parseInt(FinalProject.currentSize) - splitlocation;

        finalFile = "-1,0" + degreeOfTree +","+ splitlocation +","+ parent;
        line = bufferedReader.readLine();
        finalFile = finalFile + line +"\n";//zeroth line is copied as it is...
        secondFile = secondFile + "-1,0," +sizeOfSecondTree + ","+parent+"\n";
        line = bufferedReader.readLine();
        int i;
        firstPointerForSecondFile = "";//TODO:check here...
        for (i = 1; i <= splitlocation && line != null && Integer.parseInt(midData) > Integer.parseInt(v[1]); i++) {
            finalFile = finalFile + line + "\n";
            firstPointerForSecondFile = v[2];
            line = bufferedReader.readLine();
            v = line.split(",");
        }
        if(i <= splitlocation){
            finalFile = finalFile + i + "," + midData + "," + next + "\n";
            i++;
            for (; i <= splitlocation && line != null; i++) {
                v = line.split(",");
                finalFile = finalFile + i + "," + v[1] + "," + v[2] + "\n";
                firstPointerForSecondFile = v[2];
                line = bufferedReader.readLine();
            }
            v= line.split(",");
            secondFile = "0," + firstPointerForSecondFile + "\n";
            i++;
            newMidData = (v[1]);
            for (; i < Integer.parseInt(currentSize) + 1; i++) {
                int lineNo = i - splitlocation;
                v = line.split(",");
                secondFile = secondFile + lineNo + "," + v[1]+"," + v[2]+ "\n";
                line = bufferedReader.readLine();
            }
        } else{
            v = line.split(",");
            newMidData = (v[1]);
            secondFile = "0," + firstPointerForSecondFile + "\n";
            for (; (i < Integer.parseInt(currentSize) + 1) && Integer.parseInt(midData) > Integer.parseInt(v[1]); i++) {
                int lineNo = i - splitlocation;
                v = line.split(",");
                secondFile = secondFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                line = bufferedReader.readLine();
            }
            if (i < Integer.parseInt(currentSize) + 1) {
                secondFile = secondFile + i + "," + midData + "," + next + "\n";
                i++;
                for (; i < Integer.parseInt(currentSize) + 1; i++) {
                    int lineNo = i - splitlocation;
                    secondFile = secondFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                    line = bufferedReader.readLine();
                }
            } else {
                secondFile = secondFile + i + "," + midData + "," + next + "\n";
            }
        }

        Path thisFile = new Path(newFolderPath + "/" + currNode + ".txt");
        hdfs.delete(thisFile, true);
        hdfs.createNewFile(thisFile);
        StringBuilder sb = new StringBuilder();
        sb.append(finalFile);
        byte[] byt = sb.toString().getBytes();
        FSDataOutputStream fsOutStream = hdfs.create(thisFile);
        fsOutStream.write(byt);
        fsOutStream.close();

        Path path = new Path(newFolderPath + "/" + currNode+parent + "right.txt");
        hdfs.delete(path, true);
        hdfs.createNewFile(path);
        sb = new StringBuilder();
        sb.append(secondFile);
        byte[] bbyt = sb.toString().getBytes();
        FSDataOutputStream fssOutStream = hdfs.create(path);
        fssOutStream.write(bbyt);
        fssOutStream.close();
        next = currNode + parent + "right";
        propogate(String.valueOf(newMidData));

    }


    private static void createTree() throws IOException {
        jobConf = new JobConf(FinalProject.class);
        workingDir = hdfs.getWorkingDirectory();
        newFolderPath = new Path(workingDir, "Tree");
        if (hdfs.exists(newFolderPath)) {
            hdfs.delete(newFolderPath, true);
        }
        hdfs.mkdirs(newFolderPath);
        Path newFilePath = new Path(newFolderPath + "/1.txt");

        StringBuilder sb = new StringBuilder();
        sb.append("-1,1," + degreeOfTree + ",0" + "," + "0," + "0" + "\n");
        byte[] byt = sb.toString().getBytes();
        FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
        fsOutStream.write(byt);
        fsOutStream.close();
        System.out.println("Root node created.");
        root = "1";
        currNode = "1";
    }

}