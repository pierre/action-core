package com.ning.metrics.action.binder.config;

import org.skife.config.Config;

public class ActionCoreConfig
{
    @Config(value = "action.hadoop.namenode.url")
    public String getNamenodeUrl()
    {
        return "hdfs://127.0.0.1:9000";
    }

    @Config(value = "action.hadoop.ugi")
    public String getHadoopUgi()
    {
        return "hadoop,hadoop";
    }

    @Config(value = "action.hadoop.path")
    public String getPath()
    {
        return "/";
    }
}
