Vert.igo
========

**Need support? Check out the [Vertigo Google Group][google-group]**

**[Java User Manual](https://github.com/kuujo/vertigo/wiki/Java-User-Manual) | [Javadoc](http://vertigo.kuujo.net/java/)**

Vertigo is a fault-tolerant distributed event processing framework built on the
[Vert.x](http://vertx.io/) application platform. Following a concept and
structure similar to [Storm](https://github.com/nathanmarz/storm), Vertigo
allows real-time problems to be broken down into smaller tasks (as Vert.x
verticles) and distributed across **one or many Vert.x instances**, managing
communication between components in a **predictable and reliable** manner.

* Manages multi-step event processing systems, from simple pipelines to
  **complex networks of Vert.x modules/verticles**, including **remote procedure
  calls spanning multiple Vert.x verticle instances**
* **Abstracts communication details** from verticle implementations by providing
  an API for defining verticle relationships at the point of deployment
* **Guarantees message processing** through ack/fail/timeout mechanisms, providing
  data sources with feedback on the status of processing simple or complex
  message trees
* **Provides fault tolerant** cluster management and coordination
* Supports distribution of messages between multiple verticle instances using
  **random, round-robin, hashing, or fanout** approaches
* Supports **distribution of verticle/modules instances across a cluster** of Vert.x
  instances
* **Monitors networks for failures** and automatically reassigns/redeploys failed
  verticles and modules
* Network components can be written in **any Vert.x supported language**, with
  APIs for Vertigo 0.6 in [Javascript][vertigo-js]
  and [Python][vertigo-python]
* Integrates seemlessly with existing Vert.x applications

---

Vertigo is not a replacement for [Storm](https://github.com/nathanmarz/storm).
Rather, Vertigo is a lightweight alternative that is intended to be embedded
within larger Vert.x applications.

For an in-depth look at the concepts underlying Vertigo, check out
[how it works](https://github.com/kuujo/vertigo/wiki/How-it-works).

**[Javascript API][vertigo-js]**

**[Python API][vertigo-python]**

### Deploying a Vertigo node
```
vertx runmod net.kuujo~vertigo-cluster~0.7.0-SNAPSHOT -conf cluster.json
```

```
{
  "cluster": "vertigo.cluster",
  "address": "vertigo.cluster.1",
  "log": {
    "directory": "/tmp/log",
    "log_file": "%1.log",
    "snapshot_file": "%1.snapshot"
  }
}
```

### Adding Vertigo as a Maven dependency

```
<dependency>
  <groupId>net.kuujo</groupId>
  <artifactId>vertigo</artifactId>
  <version>0.7.0-SNAPSHOT</version>
</dependency>
```

### Including Vertigo in a Vert.x module
To use the Vertigo Java API, you can include the Vertigo module in your module's
`mod.json` file. This will make Vertigo classes available within your module.

```
{
  "main": "com.mycompany.myproject.MyVerticle",
  "includes": "net.kuujo~vertigo~0.7.0-SNAPSHOT"
}
```

**Need support? Check out the [Vertigo Google Group][google-group]**

[vertigo-python]: https://github.com/kuujo/vertigo-python
[vertigo-js]: https://github.com/kuujo/vertigo-js
[google-group]: https://groups.google.com/forum/#!forum/vertx-vertigo
