package pfjia;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 将hadoop-3.0.1-src下所有以/org开头的文件夹复制到一个文件夹中
 *
 * @author pfjia
 * @since 2018/4/3 15:38
 */
public class Main {
    public static void main(String[] args) throws IOException {
        String src = "C:\\Users\\pfjia\\Downloads\\hadoop-3.0.1-src";
        String dst = "C:\\Users\\pfjia\\Desktop\\hadoop-source-2\\";
        File file = new File(src);
        List<File> fileList = foo(file);
        System.out.println(fileList.size());
        for (File f : fileList) {
            System.out.println(f.getAbsolutePath());
            FileUtils.copyDirectory(f, new File(dst));
        }
    }


    /**
     * @param dir 根目录
     * @return dir下所有以org开头的File
     */
    private static List<File> foo(File dir) {
        if (!dir.isDirectory()) {
            return Collections.emptyList();
        }
        List<File> result = new ArrayList<>();
        //获取dir下所有目录
        File[] files = dir.listFiles(File::isDirectory);
        for (File file : files) {
            if (file.getName().equals("org")) {
                result.add(file);
            } else {
                List<File> fileList = foo(file);
                result.addAll(fileList);
            }
        }
        return result;
    }
}
