# bodhi-pipes
a data processing pipeline used in bodhi data processing

##Installation

````
npm install bodhi-pipes
````


##Usage

````
var pipes = require('bodhi-pipes')();
````


##Setup

````
var pipeline = new pipes.Pipeline('name', {/* options */})

pipeline.use(function(input, next){next(null, input)})
        .use(function(input, next){})
        .success(function handler(){})
        .fail(function handler(){})
        

````

##Data Processing

````
pipeline.execute(data)
````

