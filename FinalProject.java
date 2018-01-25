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

    public static int nodeCount=0;

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
        int numberofValues;
        reader = new Scanner(System.in);
        numberofValues = reader.nextInt();
        for(int i=0;i<numberofValues;i++){
            currNode = root;
            System.out.println("Enter Data: ");
            reader = new Scanner(System.in);
            dataKeyToBeInserted = reader.next();
            System.out.println("Enter DataString: ");
            reader = new Scanner(System.in);
            dataToBeInserted = reader.next();
            insertIntoTree();
            System.out.println("File:");
            hdfs = FileSystem.get(jobConf);
            Path newFilePath = new Path(newFolderPath + "/" + root + ".txt");
            BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
            String str;
            str = bfr.readLine();
            while ( str!= null){
                System.out.println(str);
                str = bfr.readLine();
            }
        }

        reader.close();
    }

    private static void insertIntoTree() throws IOException {
        if (root.equals("0")) {
            createTree();
            System.out.println("created tree...." + currNode + "," + parent +"," +currentSize + "," + next);
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
        if (values[1].equals("1")) {
            next = values[5];
            insertIntoLeafNode(bfr);
        } else {
            insertIntoNonLeafNode(bfr);
        }
    }

    private static void insertIntoNonLeafNode(BufferedReader bfr) throws IOException {
        int i;
        String line = bfr.readLine();//zeroth line is rea here....
        System.out.println("inserting into non leaf node line 114 : currentNode, root : " +currNode+ ", "+root );
        String[] v =line.split(",");
        String leftPointer = v[1];//leftest pointer..
        line = bfr.readLine();//first line read here....
         v =line.split(",");
            for (i = 1; i <= Integer.parseInt(currentSize) && line != null && Integer.parseInt(v[1]) < Integer.parseInt(dataKeyToBeInserted); i++) {
                leftPointer = v[2];
                line = bfr.readLine();
                if(line!=null)
                v = line.split(",");
            }
        hdfs = FileSystem.get(jobConf);
        Path newFilePath = new Path(newFolderPath + "/" + leftPointer + ".txt");
        bfr = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
        String str;
        str = bfr.readLine();
        String[] values = str.split(",");
        //currNode = root;
        currNode = leftPointer;
        currentSize = values[3];
        parent = values[4];

        if (values[1].equals("1")) {
            next = values[5];
            insertIntoLeafNode(bfr);
        } else {
            insertIntoNonLeafNode(bfr);
        }

        }



    private static void insertIntoLeafNode(BufferedReader bfr) throws IOException {

        System.out.println("inside insert into LeafNode...." + currNode + "," + parent +"," +currentSize + "," + next);

        if (Integer.parseInt(currentSize) < degreeOfTree - 1 && Integer.parseInt(currentSize)!=0) {
            System.out.println("currentSize < degree" + currNode + "," + parent +"," +currentSize + "," + next);
            String finalFile = "";
            int currSize = Integer.parseInt(currentSize)+1;
            finalFile = finalFile + -1 + ",1," + degreeOfTree +"," +currSize + "," + parent + "," + next + "\n";
            String line = bfr.readLine();
            String[] v = line.split(",");
            int data = Integer.parseInt(v[1]);
            int i;
            for (i = 0; i < currSize-1 && data < Integer.parseInt(dataKeyToBeInserted); i++) {
                finalFile = finalFile + line + "\n";
                line = bfr.readLine();
                if(line!=null) {
                    v = line.split(",");
                    data = Integer.parseInt(v[1]);
                }
            }
            if (i == currSize-1) {
                finalFile = finalFile + i + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
                i++;
            } else {
                finalFile = finalFile + i + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
                i++;
                for (; i <= currSize-1; i++) {
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
        }else if(Integer.parseInt(currentSize) == 0){
            int currSize = Integer.parseInt(currentSize) + 1;
            String finalFile = "" + -1 + ",1," + degreeOfTree + ","+currSize + "," + parent + "," + next + "\n";
            finalFile = finalFile + "0" + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
            Path newFilePath = new Path(newFolderPath + "/" + currNode + ".txt");
            hdfs.delete(newFilePath, true);
            hdfs.createNewFile(newFilePath);
            StringBuilder sb = new StringBuilder();
            sb.append(finalFile);
            byte[] byt = sb.toString().getBytes();
            FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
            fsOutStream.write(byt);
            fsOutStream.close();
        }
        else {
            splitLeaf(bfr);
        }
    }

    private static void splitLeaf(BufferedReader bfr) throws IOException {
        nodeCount++;
        int midData;
        int maxSize = degreeOfTree-1;
        int splitlocation;
        maxSize++;//it contains total size of both leafs....
        if ((maxSize+1 % 2 == 0)) {
            splitlocation = (maxSize) / 2;
        } else {
            splitlocation = ((maxSize) + 1) / 2;
        }

        Path path;
        int sizeOfSecondTree = 0;
        sizeOfSecondTree = maxSize - splitlocation;
        System.out.println("split leaf....split location,size of second tree = "+splitlocation +","+sizeOfSecondTree+","+ currNode + "," + parent +"," +currentSize + "," + next);

        String finalFile = "";
        String secondfFile = "-1,1," + degreeOfTree + "," + sizeOfSecondTree + "," + FinalProject.parent +","+ FinalProject.next + "\n";
        if (!next.equals("0")) {
            System.out.println("split leaf nex ! = 0...." + currNode + "," + parent +"," +currentSize + "," + next);
            path = new Path(newFolderPath + "/" + nodeCount + ".txt");
            finalFile = "" + -1 + "," + 1 + "," + degreeOfTree +","+ splitlocation + "," + parent + "," + nodeCount + "\n";
            next = "" + nodeCount;
        } else {
            System.out.println("split leaf nex = = 0...." + currNode + "," + parent +"," +currentSize + "," + next);
            finalFile = "" + -1 + "," + 1 + "," + degreeOfTree +","+ splitlocation + "," + parent + "," + nodeCount + "\n";
            path = new Path(newFolderPath + "/" + nodeCount + ".txt");
            next = "" + nodeCount;
        }
        String line = bfr.readLine();//zeroth line rea here....
        String[] v = line.split(",");
        int data = Integer.parseInt(v[1]);
        int i;
        for (i = 0; i < splitlocation && line != null && data < Integer.parseInt(dataKeyToBeInserted); i++) {
            finalFile = finalFile + line + "\n";
            line = bfr.readLine();
            if(line!=null) {
                v = line.split(",");
                data = Integer.parseInt(v[1]);
            }
        }
        if (i < splitlocation) {
            finalFile = finalFile + i + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
            i++;
            for (; i < splitlocation; i++) {
                finalFile = finalFile + i + "," + v[1] + "," + v[2] + "\n";
                line = bfr.readLine();
                v = line.split(",");
            }
            midData = Integer.parseInt(v[1]);
            for (; i < maxSize; i++) {
                int lineNo = i - splitlocation;
                secondfFile = secondfFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                line = bfr.readLine();
                if(line!=null)
                v = line.split(",");
            }
        } else {
            midData = Integer.parseInt(v[1]);
            if(midData>Integer.parseInt(dataKeyToBeInserted)){
                midData = Integer.parseInt(dataKeyToBeInserted);
            }
            for (; (i < maxSize-1 && data < Integer.parseInt(dataKeyToBeInserted) ); i++) {
                int lineNo = i - splitlocation;
                secondfFile = secondfFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                line = bfr.readLine();
                if(line!=null)
                v = line.split(",");
            }
            if (i < maxSize-1) {
                int lineNo = i-splitlocation;
                secondfFile = secondfFile + lineNo + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
                i++;
                for (; i < maxSize; i++) {
                    lineNo = i - splitlocation;
                    secondfFile = secondfFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                    line = bfr.readLine();
                    if(line!=null)
                        v = line.split(",");
                }
            } else {
                int lineNo = i-splitlocation;
                secondfFile = secondfFile + lineNo + "," + dataKeyToBeInserted + "," + dataToBeInserted + "\n";
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

        System.out.println("propogate....midData="+midData + "," + currNode + "," + parent +"," +currentSize + "," + next);

        if (parent.equals("0")) {//if parent is not present....create a new tree node and set it as root
            System.out.println("propogate..creating parent..midData="+midData + "," + currNode + "," + parent +"," +currentSize + "," + next);
            nodeCount++;
            Path path = new Path(newFolderPath + "/" + nodeCount + ".txt");
            System.out.println("propogate..creating parent..midData,currentNode,parent,currentSize,next,path="+midData + "," + currNode + "," + parent +"," +currentSize + "," + next + "," + path.toString());
            changeParentTo(nodeCount,currNode);
            changeParentTo(nodeCount,next);
            root = "" + nodeCount;
            System.out.println("root changed to : " + root);

            parent = "0";
             String string = "-1,0," + degreeOfTree + ",1,0" + "\n";//TODO:check here..
            string = string + "0" + "," + currNode + "\n";
            currNode = root;//TODO change its childrens to point it....
            string = string + "1," + midData + "," + next + "\n";
            hdfs.delete(path, true);
            hdfs.createNewFile(path);
            StringBuilder sb = new StringBuilder();
            sb.append(string);
            byte[] bbyt = sb.toString().getBytes();
            FSDataOutputStream fssOutStream = hdfs.create(path);
            fssOutStream.write(bbyt);
            fssOutStream.close();
            return;
        } else {
            System.out.println("propogate...parent already present.midData="+midData + "," + currNode + "," + parent +"," +currentSize + "," + next);
            Path path = new Path(newFolderPath + "/" + parent + ".txt");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            String line = bufferedReader.readLine();
            String[] v = line.split(",");
            int sizeOfParent = Integer.parseInt(v[3]);
            currNode = parent;
            parent = v[4];
            currentSize = String.valueOf(sizeOfParent);
            if (sizeOfParent < degreeOfTree-1) {
                System.out.println("propogate...sizeOfPArent < degree.midData="+midData + "," + currNode + "," + parent +"," +currentSize + "," + next);

                String finalFile = "";
                int newSize =  sizeOfParent +1;
                finalFile = "-1,0,"+degreeOfTree +","+newSize+","+parent + "\n";
                line = bufferedReader.readLine();
                finalFile = finalFile + line +"\n";//zeroth line is copied as it is...
                line = bufferedReader.readLine();//first line read here...
                v = line.split(",");
                int i;
                for (i = 1; i < newSize && line != null && Integer.parseInt(midData) > Integer.parseInt(v[1]); i++) {
                    finalFile = finalFile + line + "\n";
                    line = bufferedReader.readLine();
                    if(line!=null)
                    v = line.split(",");
                }
                if(i < newSize){
                    finalFile = finalFile + i + "," + midData + "," + next + "\n";
                    i++;
                    for (; i <= newSize && line != null; i++) {
                        finalFile = finalFile + i + "," + v[1] + "," + v[2] + "\n";
                        line = bufferedReader.readLine();
                        if(line!=null)
                        v = line.split(",");
                    }
                }else {
                    finalFile = finalFile + i + "," + midData + "," + next + "\n";
                }/*else{
                    System.out.println("some error has occured:");
                }*/
                hdfs.delete(path, true);
                hdfs.createNewFile(path);
                StringBuilder sb = new StringBuilder();
                sb.append(finalFile);
                byte[] bbyt = sb.toString().getBytes();
                FSDataOutputStream fssOutStream = hdfs.create(path);
                fssOutStream.write(bbyt);
                fssOutStream.close();
               // currNode = parent;
            } else {
                //current node is already set to this tree node, parent is set to it's parent and current size is size of this tree node..
                //midData is midValue of it's child, line is firstLine of this node and bufferReader Points to zeroth line..
                //next point to the pointer of child..
               splitTreeNode(midData,line,bufferedReader);
            }
        }
    }

    private static void changeParentTo(int nodeCount, String currNodee) throws IOException {

        Path newFilePath = new Path(newFolderPath + "/" + currNodee + ".txt");
        BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
        String str;
        String finalFile = "";
        str = bfr.readLine();
        String [] v = str.split(",");
        if(v[1].equals("1")){
            finalFile = v[0] + "," + v[1] + "," + degreeOfTree + ","  + v[3]+"," + nodeCount + "," +v[5] + "\n";
        }else{
            finalFile = v[0] + "," + v[1] + "," + degreeOfTree + "," + v[3] + "," + nodeCount + "\n";
        }
        str = bfr.readLine();
        while (str!=null){
            finalFile = finalFile + str + "\n";
            str = bfr.readLine();
        }
        System.out.println("finalFile parent changed for child :" + currNodee+ " ,to " +nodeCount );
        System.out.println("finalFile is :\n" + finalFile);
        hdfs.delete(newFilePath, true);
        hdfs.createNewFile(newFilePath);
        StringBuilder sb = new StringBuilder();
        sb.append(finalFile);
        byte[] bbyt = sb.toString().getBytes();
        FSDataOutputStream fssOutStream = hdfs.create(newFilePath);
        fssOutStream.write(bbyt);
        fssOutStream.close();

        return;
    }

    private static void splitTreeNode(String midData, String line, BufferedReader bufferedReader) throws IOException {

        //current node is already set to this tree node, parent is set to it's parent and current size is size of this tree node..
        //midData is midValue of it's child, line is -1 line of this node and bufferReader Points to zeroth line..
        //next point to the pointer of child..
        String[] v;
        System.out.println("split Tree Node" + "," + currNode + "," + parent +"," +currentSize + "," + next);
        nodeCount++;
        String finalFile = "",secondFile = "";
        int maxSize = degreeOfTree - 1;
        maxSize++;//for degree = 5, this value will be 5 and it's middle value will be propogated to top..
        String newMidData = "";//this will be propogated to top...
        String firstPointerForSecondFile;//zeroth line pointer for second file...first file has it already..
        int splitlocation;//maxSize for degree 5 is 5 at this line..
        //remember splitLocation is indexed from 1....
        if ((maxSize) % 2 == 0) {
            splitlocation = (maxSize) / 2;
        } else {
            splitlocation = ((maxSize) + 1) / 2;
        }
       // splitlocation++;//for 11 maxSize splitlocation should be 7 as splitLocation element should be in second file....
        //TODO:cross check here....
        int sizeOfSecondTree = maxSize - splitlocation+1;// for 11 elements it should be 11-7 +1;
        int sizeOfFirstNode = splitlocation-1;
        finalFile = "-1,0," + degreeOfTree +","+ sizeOfFirstNode +","+ parent;

        line = bufferedReader.readLine();//zeroth line read here...
        finalFile = finalFile + line +"\n";//zeroth line is copied as it is...
        secondFile = secondFile + "-1,0,"+degreeOfTree +","+sizeOfSecondTree + ","+parent+"\n";
        line = bufferedReader.readLine();//first line read here....
        v=line.split(",");
        int i;
        firstPointerForSecondFile = v[2];//it's right pointer of first line of first file...
        for (i = 1; i < splitlocation && line != null && Integer.parseInt(midData) > Integer.parseInt(v[1]); i++) {
            finalFile = finalFile + line + "\n";
            firstPointerForSecondFile = v[2];
            line = bufferedReader.readLine();
            v = line.split(",");
        }
        if(i < splitlocation){
            finalFile = finalFile + i + "," + midData + "," + next + "\n";
            firstPointerForSecondFile = next;
            i++;
            for (; i < splitlocation && line != null; i++) {
                finalFile = finalFile + i + "," + v[1] + "," + v[2] + "\n";
                firstPointerForSecondFile = v[2];
                line = bufferedReader.readLine();
                if(line!=null)
                v = line.split(",");
            }
            secondFile =secondFile + "0," + firstPointerForSecondFile + "\n";
            i++;
            v= line.split(",");
            newMidData = (v[1]);
            for (; i <= maxSize && line!=null; i++) {
                int lineNo = i - splitlocation+1;
                secondFile = secondFile + lineNo + "," + v[1]+"," + v[2]+ "\n";
                line = bufferedReader.readLine();
                if(line!=null)
                    v = line.split(",");
            }
        } else{
            v = line.split(",");
            newMidData = (v[1]);
            secondFile = secondFile + "0," + firstPointerForSecondFile + "\n";
            if(Integer.parseInt(midData) < Integer.parseInt(newMidData)){
                    newMidData = midData;
                    int lineNo = i-splitlocation+1;
                    secondFile = secondFile + lineNo + "," + midData + "," + next + "\n";
                    i++;
                for (; i <= maxSize && line!=null; i++) {
                    lineNo = i - splitlocation+1;
                    secondFile = secondFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                    line = bufferedReader.readLine();
                    if(line!=null)
                        v = line.split(",");
                }

            }else{
                    for (; (i < maxSize) && Integer.parseInt(midData) > Integer.parseInt(v[1]); i++) {
                        int lineNo = i - splitlocation+1;
                        secondFile = secondFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                        line = bufferedReader.readLine();
                        if(line!=null)
                        v = line.split(",");
                    }
                    if (i < maxSize) {
                        int lineNo = i - splitlocation+1;
                        secondFile = secondFile + lineNo + "," + midData + "," + next + "\n";
                        i++;
                        for (; i <= maxSize && line!=null; i++) {
                           lineNo = i - splitlocation+1;
                            secondFile = secondFile + lineNo + "," + v[1] + "," + v[2] + "\n";
                            line = bufferedReader.readLine();
                            if(line!=null)
                            v = line.split(",");
                        }
                    } else {
                        int lineNo = i - splitlocation+1;
                        secondFile = secondFile + lineNo + "," + midData + "," + next + "\n";
                    }

            }

        }
        //next = String.valueOf(nodeCount);
        Path thisFile = new Path(newFolderPath + "/" + currNode + ".txt");
        hdfs.delete(thisFile, true);
        hdfs.createNewFile(thisFile);
        StringBuilder sb = new StringBuilder();
        sb.append(finalFile);
        byte[] byt = sb.toString().getBytes();
        FSDataOutputStream fsOutStream = hdfs.create(thisFile);
        fsOutStream.write(byt);
        fsOutStream.close();

        Path path = new Path(newFolderPath + "/" + nodeCount + ".txt");
        next = String.valueOf(nodeCount);
        hdfs.delete(path, true);
        hdfs.createNewFile(path);
        sb = new StringBuilder();
        sb.append(secondFile);
        byte[] bbyt = sb.toString().getBytes();
        FSDataOutputStream fssOutStream = hdfs.create(path);
        fssOutStream.write(bbyt);
        fssOutStream.close();
        propogate(String.valueOf(newMidData));

    }


    private static void createTree() throws IOException {
        System.out.println("inside create tree" + "," + currNode + "," + parent +"," +currentSize + "," + next);
        nodeCount++;
        jobConf = new JobConf(FinalProject.class);
        workingDir = hdfs.getWorkingDirectory();
        newFolderPath = new Path(workingDir, "Tree");
        if (hdfs.exists(newFolderPath)) {
            hdfs.delete(newFolderPath, true);
        }
        hdfs.mkdirs(newFolderPath);
        Path newFilePath = new Path(newFolderPath + "/"+nodeCount +".txt");

        StringBuilder sb = new StringBuilder();
        sb.append("-1,1," + degreeOfTree + ",0" + "," + "0," + "0" + "\n");
        byte[] byt = sb.toString().getBytes();
        FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
        fsOutStream.write(byt);
        fsOutStream.close();
        System.out.println("Root node created.");
        root = String.valueOf(nodeCount);
        currNode = root;
    }

}