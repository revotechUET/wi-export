let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../space');
let wiImport = require('wi-import');
var csv = require('fast-csv');
let hashDir = wiImport.hashDir;


async function writeCurve(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, s3, curveModel, curveBasePath, callback) {
    console.log('write csv called');
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, s3, curveModel are null
    */
    if (!well) {  //export from project
        well = project.wells[0];
        well.username = project.createdBy;
    }

    let top = Number.parseFloat(dataset.top);
    let bottom = Number.parseFloat(dataset.bottom);
    let step = Number.parseFloat(dataset.step);
    let readStreams = [];
    var csvStream = csv.createWriteStream({ headers: false });
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' });
    csvStream.pipe(writeStream);
    let countCurve = 0;
    let curveNameArr = [];
    curveNameArr.push('MD');

    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) { return curve.idCurve == idCurve });
        if (curve) {
            countCurve++;
            curveNameArr.push(curve.name);
            if (!project) { //export from inventory
                let curvePath = await curveModel.getCurveKey(curve.curve_revisions[0]);
                console.log('curvePath=========', curvePath);
                let stream = await s3.getData(curvePath);
                stream = byline.createStream(stream);
                readStreams.push(stream);
            } else { //export from project
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                let stream = fs.createReadStream(curvePath);
                stream = byline.createStream(stream);
                readStreams.push(stream);
            }
        }
    }

    if (readStreams.length === 0) {
        console.log('hiuhiu');
        csvStream.write(curveNameArr);
        for (let i = top; i < bottom + step; i += step) {
            csvStream.write([i.toFixed(4)]);
            if (i >= bottom) {
                callback(null, {
                    fileName: fileName,
                    wellName: well.name,
                    datasetName: dataset.name
                })
            }
        }
    } else {
        csvStream.write(curveNameArr);
        let tokenArr = [];
        for (let i = 0; i < readStreams.length; i++) {
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].on('data', function (line) {
                readLine++;
                let tokens = line.toString('utf8').split("||");
                tokens = tokens.toString().substring(tokens.toString().indexOf(" ") + 1);
                if (tokens == 'null') {
                    let nullHeader = well.well_headers.find(header => {
                        return header.header == "NULL";
                    })
                    tokens = nullHeader ? nullHeader.value : 'null';
                }
                if (i === 0) {
                    let depth = top.toFixed(4).toString();
                    tokenArr.push(depth);
                    top += step;
                }
                tokenArr.push(tokens);
                if (i !== readStreams.length - 1) {
                    readStreams[i].pause();
                    if (readStreams[i + 1].isPaused()) {
                        readStreams[i + 1].resume();
                    }
                } else {
                    csvStream.write(tokenArr, function () {
                        writeLine++;
                        if (readStreams.numLine && readStreams.numLine === writeLine) {
                            csvStream.end();
                            writeStream.on('finish', function () {
                                callback(null, {
                                    fileName: fileName,
                                    wellName: well.name,
                                    datasetName: dataset.name
                                });
                            }) 
                        }
                    });
                    tokenArr = [];
                    readStreams[i].pause();
                    if (readStreams[0].isPaused()) {
                        readStreams[0].resume();
                    }
                }
            })
            readStreams[i].on('end', function () {
                if(!readStreams.numLine) {
                    readStreams.numLine = readLine;                    
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

function writeAll(exportPath, project, well, idDataset, idCurves, username, s3, curveModel, curveBasePath, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, s3, curveModel are null
    */
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

    let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == idDataset; });
    if (dataset) {
        let fileName = dataset.name + "_" + well.name + "_" + Date.now() + '.csv'
        fileName = fileName.replace(/\//g, "-");
        lasFilePath = path.join(lasFilePath, fileName);

        if (project) { //export from project
            writeCurve(lasFilePath, exportPath, fileName, project, null, dataset, idCurves, null, null, curveBasePath, function (err, rs) {
                console.log('writeAll callback called', rs);
                if (err) {
                    callback(err);
                } else {
                    callback(null, rs);
                }
            });
        } else { //export from inventory
            writeCurve(lasFilePath, exportPath, fileName, null, well, dataset, idCurves, s3, curveModel, null, function (err, rs) {
                console.log('writeAll callback called', rs);
                if (err) {
                    callback(err);
                } else {
                    callback(null, rs);
                }
            });
        }
    } else {
        console.log('no dataset');
        callback(null, null);
    }
}
module.exports.writeAll = writeAll;