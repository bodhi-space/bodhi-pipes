module.exports = function pipelines(defaults){

    defaults = defaults || {};

    var EventEmitter = require('events').EventEmitter,
        async        = require('async'),
        util         = require('util'),
        extend       = require('deep-extend'),
        shortid      = require('shortid'),
        _slice       = Array.prototype.slice;

    var NAME = 'anonymous';

    //- Defaults
    defaults = extend({}, {
        workers: 5
    }, defaults);

    function isFunction(value){
        return typeof value == 'function' || false;
    }

    function isString(value){
        return typeof value == 'string' || false;
    }

    function isObject(value){
        return typeof value == 'object' || false;
    }

    var functions = (function() {
        'use strict';

        function functionName(fun, anonymous) {
            anonymous = anonymous || '<anonymous>';
            if(!isFunction(fun)){
                return 'NaF';
            } else {
                var ret = fun.toString();
                ret = ret.substr('function '.length);
                ret = ret.substr(0, ret.indexOf('('));
                ret = (!ret && ret.length < 1) ? anonymous : ret;
                return ret;
            }
        }

        function functionParameters(func) {

            //Check the arguments
            if(!isFunction(fun)){
                return [];
            }

            var STRIP_COMMENTS = /((\/\/.*$)|(\/\*[\s\S]*?\*\/))/mg;
            var fnStr = func.toString().replace(STRIP_COMMENTS, '');
            var result = fnStr.slice(fnStr.indexOf('(') + 1, fnStr.indexOf(')')).match(/([^\s,]+)/g);
            if (result === null)
                result = [];
            return result;
        }

        return {
            name        : functionName,
            parameters  : functionParameters
        }

    })();



    var SIGBREAK = {};

    //States
    var _BROKE   = 'stopped';
    var _SUCCESS = 'success';
    var _FAIL    = 'failed';
    var _DONE    = 'done';

    function FlowContext(pipeline, envelope){
        this.id       = shortid.generate();
        this.name     = pipeline.getName();
        this.steps    = 0;
        this.stack    = [];
        this.started  = Date.now();
        this.done     = false;
        this.envelope = envelope;
    }

    FlowContext.prototype = {

        broke: function(){
            this.result = _BROKE;
        },

        succeeded: function(){
            this.result = _SUCCESS;
        },

        failed: function(){
            this.result = _FAIL;
        },

        push: function(filter){
            var fn = filter.fn;
            this.steps   = this.steps + 1;
            this.step    = functions.name(fn, "step[" + this.steps + "]");
            this.stack.push(this.step);
            return filter;
        },

        complete: function(status, result){
            if(!this.done){
                this.done        = true;
                this.disposition = status;
                this.stopped     = Date.now();
                this.duration    = (this.stopped - this.started)
            }
            return this;
        }
    };

    /**
     * Pipeline
     *
     * @param name
     * @constructor
     */

    util.inherits(Pipeline, EventEmitter);

    //ideally attach the context to the options (name: context: concurrent: )
    function Pipeline(name, options) {
        'use strict';

        var pipeline        = this;
        pipeline.filters    = [];
        EventEmitter.call(pipeline);

        if(arguments.length === 1 && isObject(name)){
            options = name;
            name = NAME;
        }

        options = options || {};
        options = extend(defaults, options);

        var queue = async.queue(function(message, callback){
            var flowCtx = new FlowContext(pipeline, message.headers);
            _process(message.message, flowCtx, function(err, result, flowCtx){
                pipeline.after((err) ? err : result, flowCtx);
                callback(err);
            })
        }, options.workers || DEFAULT_WORKERS);

        pipeline.getName = function getName(){
            return (name || NAME);
        };

        //Add middleware to the system
        pipeline.use = function(filter, context) {
            if(isFunction(filter)){
                pipeline.filters.push({ fn: filter, context: context });
            }
            return pipeline;
        };

        /**
         * Configures a breaker - Break IF function returns 'truthy'
         *
         * @param predicate
         * @returns {pipeline}
         */
        pipeline.breakIf = function(predicate) {
            pipeline.use( function(message, next) {
                if (predicate(message)) {
                    return next(null, SIGBREAK);
                }
                next(null, message);
            });
            return pipeline;
        };

        pipeline.before = function(meta){
            return this.emit('before', meta);
        };

        pipeline.execute = function execute(message, headers) {
            queue.push({message: message, headers: headers});
        };

        pipeline.after = function(result, meta){
            pipeline.emit(meta.disposition, result, meta);
            return pipeline.emit(_DONE, meta);
        };

        function enhanceFilterContext(context){
            context.$BREAKER = SIGBREAK;
            return context;
        }

        function _process(message, flowCtx, callback){
            var    pending = _slice.call(pipeline.filters || []);

            var continueExecution = function processingLoop(err, result) {

                //- fail
                if (err) {
                    return callback(null, err, flowCtx.complete(_FAIL));
                }

                //- break
                if (result === SIGBREAK) {
                    return callback(null, result, flowCtx.complete(_BROKE));
                }

                //- success
                if (pending.length === 0) {
                    return callback(null, result, flowCtx.complete(_SUCCESS));
                }

                // take next filter from pending list, and continue execution
                var filter = flowCtx.push(pending.shift());
                process.nextTick(function() {
                    try{
                        filter.fn.call(extend(enhanceFilterContext(filter.context || {}), {
                            headers: flowCtx.envelope,
                            msg_id : flowCtx.id
                        }), result, continueExecution);
                    } catch (err) {
                        continueExecution(err);
                    }
                });
            };

            process.nextTick(function pipelineStep() {
                continueExecution(null, message);
            });
        }

        /**
         * Add a break handler
         *
         * function(lastState, flowContext)
         *
         * @param cb
         */
        pipeline.broke = pipeline.stopped = function(handler, ctx) {
            ctx = ctx || {};
            pipeline.on(_BROKE  , function(){
                return handler.call(ctx, arguments[0], arguments[1]);
            });
            return pipeline;
        };

        /**
         * Add a success handler
         *
         * function(result, flowContext)
         *
         */
        pipeline.success = function(handler, ctx) {
            ctx = ctx || {};
            pipeline.on(_SUCCESS  , function(){
                return handler.call(ctx, arguments[0], arguments[1]);
            });
            return pipeline;
        };

        /**
         * Add a errback handler
         *
         * function(err, flowContext)
         *
         *
         * @param cb
         */
        pipeline.fail = pipeline.failed = function(handler, ctx) {
            ctx = ctx || {};
            pipeline.on(_FAIL , function(){
                return handler.call(ctx, arguments[0], arguments[1]);
            });
            return pipeline;
        };

        pipeline.always = function(handler, ctx) {
            ctx = ctx || {};
            pipeline.on(_DONE  , function _always(){
                return handler.call(ctx, arguments[0]);
            });
            return pipeline;
        };

        pipeline.done = pipeline.always;

        /**
         * Shutdown the pipeline
         *
         */
        pipeline.close = function(callback) {

            var emitter  = this;
            pipeline.queue.kill();

            var removeListeners = function() {
                emitter.removeListener(_FAIL    , error);
                emitter.removeListener(_SUCCESS , end);
                emitter.removeListener(_BROKE   , removeListeners);
            };

            var error = function(err) {
                removeListeners();
                done(err);
            };

            var end = function(result) {
                removeListeners();
                done(null, result);
            };

            this.once(_FAIL   , error);
            this.once(_SUCCESS, end);
            this.once(_BROKE  , removeListeners);
            this.once(_DONE   , removeListeners);

        };


        //- Representations
        pipeline.toJSON = function(){

            var filters = [];
            for(var i = 0; i < pipeline.filters.length; i++){
                var fn = pipeline.filters[i].fn;
                filters.push(functions.name(fn, "step[" + (i+1) + "]"));
            }

            return JSON.stringify({
                type   : 'Pipeline',
                name   : pipeline.getName(),
                workers: options.workers,
                filters: filters
            }, null, '');

        };

        pipeline.toJson = pipeline.toJSON;

        pipeline.toString = function(){
            return 'Pipeline:' + pipeline.getName();
        };

    }

    return {
        BREAKER       : SIGBREAK,
        Pipeline      : Pipeline
        , create      : function create(name , ns, options) {

            var _name    = NAME;
            var _options = {};

            if(arguments === 0){
                name    = _name;
                options = {}
            } else if(arguments === 1){
                options = arguments[0] || _options;
                name    = options.name || _name;
            } else if (arguments === 2){
                name    = arguments[0] || _name;
                options = arguments[1] || _options;
            } else {
                name    = arguments[0]  || _name;
                ns      = (isObject(arguments[1])) ? arguments[1].name : arguments[1];
                options = arguments[2] || _options;
            }

            name = (ns) ? ns + '/' + name : name;

            return new Pipeline(name , options);
        }
    }

};