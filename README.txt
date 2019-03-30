1. storm系统资源文件打包时候请注意排除，比如logback.xml
2. 自定义配置mvn会打包进入dist-conf
3. 本地运行：1)jstorm-core scope注释掉， 2)storm.cluster.mode: "local"
