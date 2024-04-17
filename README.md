# SequoiaDB 连接器

SequoiaDB 连接器，包含 flink、spark 两种连接器

## 工程介绍

```
├── flink        # flink 连接器代码
├── spark        # spark2 连接器代码
└── spark-3.0    # spark3 连接器代码
```

**说明**: spark3 连接器不建议在生产环境中使用

## 编译环境

- Java version: 1.8+

## 编译

```
./compile.sh --type all
```

**说明**: 更多用法请参考 `./compile.sh -h`

编译产物存放与 build 目录下

```
tree ./build

./build/
├── sdb-flink-connector-5.12.1-beta.jar       # flink 连接器 jar 包
├── sdb-flink-connector-5.12.1-beta.tar.gz    # flink 连接器压缩包
├── spark-sequoiadb_2.11-5.12.1-beta.jar      # spark2 连接器 jar 包
├── spark-sequoiadb_3.0-5.12.1-beta.jar       # spark3 连接器 jar 包
└── spark-sequoiadb-5.12.1-beta.tar.gz        # spark 连接器压缩包
```

## 版本说明

* 在 v5.10 分支中
   * flink 连接器支持 flink 1.14+
   * spark2 连接器支持 spark 2.0.0+
   * spark3 连接器支持 spark 3.0.0+

* 在 master 分支（v5.12）中
   * flink 连接器支持 flink 1.17.2+
   * spark2 连接器支持 spark 2.0.0+
   * spark3 连接器支持 spark 3.0.0+