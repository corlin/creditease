package cn.creditease.forecast.sample;

/**
 * Created by corlinchen on 2017/4/20.
 */
import java.util.ServiceLoader;

import org.apache.hadoop.hbase.util.Bytes;


public class BeanFactory {

    private BeanFactory() {
    }

    public static BeanFactory getInstance() {
        return BeanFactoryHolder.instance;
    }

    public  <T> T getBeanInstance(Class<T> type) {
        ServiceLoader<T> serviceLoad = ServiceLoader.load(type, getInstance()
                .getClass().getClassLoader());
        if (serviceLoad != null) {
            //System.out.println(serviceLoad.toString());
            for (T ele : serviceLoad) {
                //System.out.println(ele.getClass().getName());
                return ele;
            }
        }
        return null;

    }

    private static class BeanFactoryHolder {
        private static final BeanFactory instance = new BeanFactory();
    }

    public static void main(String[] args) {
        System.out.println(Bytes.toStringBinary(BeanFactory.getInstance().getBeanInstance(
                RowKeyGenerator.class).nextId()));

    }

}
