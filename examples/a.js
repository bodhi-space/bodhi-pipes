var pipelines = require('../')();
console.log(pipelines);

var p = [
    //pipelines.create(),
    //pipelines.create({}),
    //pipelines.create('a', {}),
    //pipelines.create('a', 'b', {}),
    //new pipelines.Pipeline(),
    new pipelines.Pipeline({}),
    //new pipelines.Pipeline('a'),
    //new pipelines.Pipeline('a', {})
];

for(var i = 0; i < p.length; i++){
    console.log(p[i].toJson());
}