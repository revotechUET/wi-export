let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
var csv = require('fast-csv');
let space = require('../../space');
let wiImport = require('wi-import');
const MDCurve = '__MD';
let hashDir = wiImport.hashDir;

function writeHeader(csvStream, well, idCurves) {
    console.log('---------idCurves', idCurves);
    let headerArr = ['$Csv :WELL ', 'Dataset'];
    headerArr.push(well.name)
    csvStream.write(headerArr);
    csvStream.write([]);

    let columnArr = ['WELL', 'Dataset', 'Depth'];
    let unitArr = ['.', '.', 'M'];
    async.eachOfSeries(well.datasets, function (dataset, index, nextDataset) {
        async.eachOfSeries(dataset.curves, function (curve, idx, nextCurve) {
            if(idCurves.find(function (id) {return id == curve.idCurve})) {
                console.log('curve', curve.name, curve.unit);
                columnArr.push(curve.name);
                unitArr.push(curve.unit);
            }
            nextCurve();
        }, function (err) {
            nextDataset();
        })
    }, function (err) {
        if (err) {
            console.log('writeHeader err', err);
        }
        csvStream.write(columnArr);
        csvStream.write(unitArr);
    })
}

async function writeDataset(csvStream, writeStream, project, well, dataset, idCurves, numOfPreCurve, s3, curveModel, curveBasePath, callback) {

    let top = Number.parseFloat(dataset.top);
    let bottom = Number.parseFloat(dataset.bottom);
    let step = Number.parseFloat(dataset.step);
    let readStreams = [];

    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) { return curve.idCurve == idCurve });
        if (curve && curve.name != MDCurve) {
            let stream;
            if (!project) { //export from inventory
                let curvePath = await curveModel.getCurveKey(curve.curve_revisions[0]);
                console.log('curvePath=========', curvePath);
                try {
                    stream = await s3.getData(curvePath);
                } catch (e) {
                    console.log('=============NOT FOUND CURVE FROM S3', e);
                    callback(e);
                }
            } else { //export from project
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                stream = fs.createReadStream(curvePath);
            }
            stream = byline.createStream(stream).pause();
            readStreams.push(stream);
        }
    }

    if (readStreams.length === 0) {
        console.log('hiuhiu');
        for (let i = top; i < bottom + step; i += step) {
            let lineArr = generateLineArr(well.name, dataset.name, i.toFixed(4), numOfPreCurve);
            csvStream.write(lineArr);
            if (i >= bottom) {
                callback();
            }
        }
    } else {
        readStreams[0].resume();
        let lineArr;
        for (let i = 0; i < readStreams.length; i++) {
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].on('data', function (line) {
                readLine++;
                let tokens = line.toString('utf8').split("||");
                tokens = tokens.toString().substring(tokens.toString().indexOf(" ") + 1);
                if (tokens == null || tokens == NaN || tokens == 'null' || tokens == 'NaN') {
                    // let nullHeader = well.well_headers.find(header => {
                    //     return header.header == "NULL";
                    // })
                    // tokens = nullHeader ? nullHeader.value : '-999.0000';
                    tokens = '-999';
                }
                if (i === 0) {
                    let depth = top.toFixed(4).toString();
                    lineArr = generateLineArr(well.name, dataset.name, depth, numOfPreCurve);
                    top += step;
                }
                lineArr.push(tokens);
                if (i !== readStreams.length - 1) {
                    readStreams[i].pause();
                    if (readStreams[i + 1].isPaused()) {
                        readStreams[i + 1].resume();
                    }
                } else {
                    csvStream.write(lineArr, function () {
                        writeLine++;
                        if (readStreams.numLine && readStreams.numLine === writeLine) {
                            callback();
                        }
                    });
                    lineArr = generateLineArr(well.name, dataset.name, numOfPreCurve);
                    readStreams[i].pause();
                    if (readStreams[0].isPaused()) {
                        readStreams[0].resume();
                    }
                }
            })
            readStreams[i].on('end', function () {
                if (!readStreams.numLine) {
                    readStreams.numLine = readLine;
                }
                if (i == readStreams.length - 1 && readLine == 0) {
                    callback('No curve data');
                }
                console.log('END TIME', new Date(), readStreams.numLine);
                if (i != readStreams.length - 1) {
                    console.log('---', i, readStreams.length - 1);
                    readStreams[i + 1].resume();
                }
            })
        }
    }
}

function writeAll(exportPath, project, well, datasetObjs, username, s3, curveModel, curveBasePath, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, s3, curveModel are null
    */
    console.log('WriteAll exportPath ----', exportPath);
    if (!well) { //export from inventory
        well = project.wells[0];
    }

    //create path if not existed
    if (!fs.existsSync(exportPath)) {
        fs.mkdirSync(exportPath);
    }
    let lasFilePath = path.join(exportPath, username);
    if (!fs.existsSync(lasFilePath)) {
        fs.mkdirSync(lasFilePath);
    }


    let fileName = well.name + "_" + Date.now() + '.csv'
    fileName = fileName.replace(/\//g, "-");
    lasFilePath = path.join(lasFilePath, fileName);

    var csvStream = csv.createWriteStream({ headers: false });
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' });
    let idCurvesArr = [];
    csvStream.pipe(writeStream);
    writeHeader(csvStream, well, getIdCurvesArr(datasetObjs));

    let numOfPreCurve = 0;
    async.eachOfSeries(datasetObjs, function (obj, index, next) {
        let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == obj.idDataset; });
        writeDataset(csvStream, writeStream, project, well, dataset, obj.idCurves, numOfPreCurve, s3, curveModel, curveBasePath, function (e) {
            if (e) { console.log(e); }
            numOfPreCurve += obj.idCurves.length;
            next();
        });
    }, function (err) {
        if(err) {
            callback(err);
        } else {
            csvStream.end();
            writeStream.on('finish', function () {
                callback(null, {
                    wellName: well.name,
                    fileName: fileName
                })
            })
        }
    })
}

function generateLineArr(wellName, datasetName, depth, numOfPreCurve) {
    let arr = [];
    arr.push(wellName);
    arr.push(datasetName);
    arr.push(depth);
    for (let i = 0; i < numOfPreCurve; i++) {
        arr.push("");
    }
    return arr;
}

function getIdCurvesArr (datasetObjs) {
    let arr = [];
    async.eachOfSeries(datasetObjs, function (obj, index, next) {
        async.eachOfSeries(obj.idCurves, function(id, idx, nextId) {
            arr.push(id);
            nextId();
        }, function () {
            next();
        }, function () {
            console.log('ok');
        })
    })
    return arr;
}

module.exports.writeAll = writeAll;
