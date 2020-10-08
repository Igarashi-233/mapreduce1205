package com.myfileinputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RecordReader, 只能处理一个文件
 * 将文件直接读成一个<K, V>值
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;
    private FileSplit fs;

    /**
     * 初始化方法  框架在开始时调用一次
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //转换切片类型到文件切片
        fs = (FileSplit) split;
        //通过切片获取路径
        Path path = fs.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        //开流
        inputStream = fileSystem.open(path);
    }

    /**
     * 读取下一组<k, v>值
     * @return 读取到了返回true, 读取结束返回false
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(notRead){
            //具体读取文件过程
            //读取KEY
            key.set(fs.getPath().toString());

            //读取VALUE
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf, 0, buf.length);

            notRead = false;
            return true;
        }else {
            return false;
        }
    }

    /**
     * 获取当前的KEY
     * @return 当前的KEY
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前的VALUE
     * @return 当前的VALUE
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 当前数据的读取进度
     * @return 当前进度(0.0-1.0)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}
