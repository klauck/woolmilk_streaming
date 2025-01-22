# WoolMilk Streaming

In this repository, we investigate how to run streaming queries in a multi-node cluster using [Apache DataFusion](https://datafusion.apache.org/), or more general in a composed data management system.
The idea of using DataFusion is that we can "spend most time implementing value-adding features rather than replicating existing analytic engine technologies" [1].


## Resources

### Composable Data Management Systems

  - Pedreira et al.: The Composable Data Management System Manifesto
    
    https://www.vldb.org/pvldb/vol16/p2679-pedreira.pdf

  - [1] Lamb et al.: Apache Arrow DataFusion: a Fast, Embeddable, Modular Analytic Query Engine
    
    http://andrew.nerdnetworks.org/other/SIGMOD-2024-lamb.pdf
  
  - Khurana et al.: The Modern Data Architecture: The Deconstructed Database

    https://www.usenix.org/system/files/login/articles/login_winter18_08_khurana.pdf


    #### DataFusion Streaming

    - [GitHub issue](https://github.com/apache/datafusion/issues/1544)
    - [Synnada](https://www.synnada.ai/)
    - [Arroyo](https://www.arroyo.dev/)

### Benchmarks

  - NEXMark

    [Queries](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/)

  - Linear Road

    [Queries](http://infolab.stanford.edu/stream/cql-benchmark.html)
