# disruptor-worker

[![](https://jitpack.io/v/benlocal/disruptor-worker.svg?style=flat-square)](https://jitpack.io/#benlocal/disruptor-worker)
![Weekly download statistics](https://jitpack.io/v/benlocal/disruptor-worker/week.svg)
![Monthly download statistics](https://jitpack.io/v/benlocal/disruptor-worker/month.svg)

### Used

1. Add the JitPack repository to your build file

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

2. Add the dependency

- disruptor-worker

```xml
<dependency>
    <groupId>com.github.benlocal.disruptor-worker</groupId>
    <artifactId>disruptor-worker</artifactId>
    <version>main-SNAPSHOT</version>
</dependency>
```

- disruptor-worker-springboot-starter

```xml
<dependency>
    <groupId>com.github.benlocal.disruptor-worker</groupId>
    <artifactId>disruptor-worker-springboot-starter</artifactId>
    <version>main-SNAPSHOT</version>
</dependency>
```

- disruptor-worker-store-jdbc

```xml
<dependency>
    <groupId>com.github.benlocal.disruptor-worker</groupId>
    <artifactId>disruptor-worker-store-jdbc</artifactId>
    <version>main-SNAPSHOT</version>
</dependency>
```

### Build

- Skip test

```bash
mvn clean install -Dmaven.test.skip=true
```
