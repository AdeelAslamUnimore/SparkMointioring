package org.example;

import com.fasterxml.jackson.module.scala.ser.SymbolSerializer;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.scheduler.*;
import scala.Option;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;

public class Model extends org.apache.spark.scheduler.SparkListener{
   // Option<String> stringOption;

    private String applicationName;

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        super.onExecutorAdded(executorAdded);
        try{
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets))
                displayInterfaceInformation(netint);
            System.exit(-1);
        }catch (SocketException e){

        }
        }
    static void displayInterfaceInformation(NetworkInterface netint) throws SocketException {
        System.out.printf("Display name: %s\n", netint.getDisplayName());
       System. out.printf("Name: %s\n", netint.getName());
        Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
        for (InetAddress inetAddress : Collections.list(inetAddresses)) {
            System.out.printf("InetAddress: %s\n", inetAddress);
        }
        System.out.printf("\n");
    }
    
}
