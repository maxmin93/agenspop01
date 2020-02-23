# Agenspop

Agenspop is graph framework and visualization for Big-graph using Elasticsearch.
As you know, Agenspop is build based on Apache Tinkerpop.

This project has two sub-module
1) frontend : visualization tool
2) backend : agenspop server connecting elasticsearch

Before run agenspop, you should setup elasticsearch.
- modify es-config.yml for connecting your ES
 
## _`agenspop`_ main features

## Rest-API

### admin api
- http://<host:port>/api/admin/graphs
- http://<host:port>/api/admin/labels/modern
- http://<host:port>/api/admin/keys/modern/person

### search api
- http://<host:port>/api/search/modern/v
- http://<host:port>/api/search/modern/e

### gremlin api
- http://<host:port>/api/graph/gremlin?q=modern_g.V().has(%27age%27,gt(30))
- http://<host:port>/api/graph/gremlin?q=modern_g.V().has(%27name%27,%20%27marko%27).out().out().valueMap()
- http://<host:port>/api/graph/gremlin?q=northwind_g.V().groupCount().by(T.label)
- http://<host:port>/api/graph/gremlin?q=northwind_g.V().hasLabel(%27product%27).properties().key().groupCount()
- http://<host:port>/api/graph/gremlin?q=northwind_g.E().project("self","inL","outL").by(__.label()).by(__.inV().label()).by(__.outV().label()).groupCount()

### cypher api
- http://<host:port>/api/graph/cypher?ds=modern&q=match%20(a:person%20%7Bcountry:%20%27USA%27%7D)%20return%20a,%20id(a)%20limit%2010


### ElasticVertex, ElasticEdge

processing transaction with traversaling vertices and edges

### logstash filter for agenspop

...
 
### AgensWorkspace

This has two mode that are canvas and webgl.
At first, graph data will be loaded on webgl because webgl is more strong and fast.
And you can crop what you want look detail on webgl. 
Cropped graph will be loaded on canvas. 
canvas is powerful with many functions.   


I hope enjoy agenspop.
