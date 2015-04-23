module.exports = function(grunt) {

    // Add the grunt-mocha-test tasks.
    grunt.loadNpmTasks('grunt-mocha-test');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-bump');
    grunt.loadNpmTasks('grunt-mkdir');
    grunt.loadNpmTasks('grunt-exec');
    grunt.loadNpmTasks('grunt-bump');
    grunt.loadNpmTasks('grunt-sloc');
    grunt.loadNpmTasks('grunt-conventional-changelog');

    var BUILD_DIR   = './dist';
    var RELEASE_DIR = './dist';

    grunt.initConfig({

        pkg : grunt.file.readJSON( 'package.json' ),

        // Configure a mochaTest task
        mochaTest: {
            smoke: {
                options: {
                    reporter: 'spec'
                },
                src: ['test/**/*.js']
            }
        },
        //- Build Release Tasks
        mkdir: {
            all: {
                options: { create: [BUILD_DIR] }
            }
        },
        //- Clean up previous builds
        clean: {
            build: {
                src: [ BUILD_DIR ]
            },
            coverage: {
                src: ['coverage/']
            }
        },
        //- Copy the build
        copy: {
            build: {
                files: [
                    { src: [ './lib/**'          ], dest: BUILD_DIR, expand: true },
                    { src: [ './bin/**'          ], dest: BUILD_DIR, expand: true },
                    { src: [ 'package.json'      ], dest: BUILD_DIR, expand: true },
                    { src: [ 'index.js'          ], dest: BUILD_DIR, expand: true },
                    { src: [ 'README.md'         ], dest: BUILD_DIR, expand: true },
                    { src: [ 'license.txt'       ], dest: BUILD_DIR, expand: true },
                    { src: [ 'release-notes.txt' ], dest: BUILD_DIR, expand: true }
                ]
            }
        },
        exec: {
            lsr        : { cmd: 'ls -la', cwd: 'release' }
            , npm_release: { cmd: 'npm install --production', cwd: 'release' }
            , more_pkg   : { cmd: 'more package.json', cwd: 'release' }
            , prune_pkg  : { cmd: function(){
                var rPkg = grunt.file.readJSON( 'package.json' )
                delete rPkg.devDependencies;
                delete rPkg.scripts;
                grunt.file.write(BUILD_DIR + '/package.json', JSON.stringify(rPkg, null, 4));
                return "";
            }}
        },
        changelog: {
            options: {
                commitLink: function(commitHash){ return "https://bitbucket.org/redbookplatform/rbc-agent" + '/commits/' + commitHash; }
                , from: "de45ea5124f8f4e060bfcea7ccc0877fb5fcbcd3"
            }
        },
        sloc: {
            options: {
                reportDetail: true
            },
            core: {
                files: {
                    'lib': [ '**' ]
                }
            },
            dist: {
                files: {
                    'lib': [ '**' ]
                }
            },
            tests: {
                files: {
                    'test': [ '**' ]
                }
            }
        },
        //Bump the version number
        bump: {
            options: {
                files: ['package.json'],
                updateConfigs: [],
                commit: true,
                commitMessage: 'Release v%VERSION%',
                commitFiles: ['package.json'],
                createTag: true,
                tagName: 'V%VERSION%',
                tagMessage: 'Version %VERSION%',
                push: true,
                pushTo: 'origin',
                gitDescribeOptions: '--tags --always --abbrev=1 --dirty=-d'
            }
        }
    });

    grunt.registerTask('count',
        'count the code in the core lib',
        ['sloc:core']
    );

    grunt.registerTask('test',
        'basic check of the codebase',
        ['mochaTest:smoke']
    );

    //Build a Basic Distribution
    grunt.registerTask('build',
        'Compiles all of the assets and copies the files to the build directory.',
        [ 'clean', 'mkdir', 'copy', 'exec:prune_pkg', 'sloc:dist']
    );

    grunt.registerTask('release',
        'Creates a release',
        [ 'changelog', 'clean', 'mkdir', 'copy', 'bump', 'exec:prune_pkg']
    );

};
