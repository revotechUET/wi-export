let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../space');
let wiImport = require('wi-import');
let hashDir = wiImport.hashDir;

function writeVersion(lasFilePath) {
    fs.appendFileSync(lasFilePath, '~Version\r\n');
    fs.appendFileSync(lasFilePath, 'VERS  .             3.00                                : CWLS LOG ASCII STANDARD - VERSION 3.00\r\n');
    fs.appendFileSync(lasFilePath, 'WRAP  .             NO                                  : One line per depth step\r\n');
    fs.appendFileSync(lasFilePath, 'DLM   .             COMMA                               : Delimiter character between data columns\r\n');
    fs.appendFileSync(lasFilePath, '#Delimiting character ( SPACE TAB OR COMMA ).\r\n\r\n');
}

function writeWellHeader(lasFilePath, wellHeaders) {
    fs.appendFileSync(lasFilePath, '~Well\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT          DATA                                DESCRIPTION\r\n');
    fs.appendFileSync(lasFilePath, '#----- ----        -----------------                   -----------\r\n');
    //append start depth, stop depth and step
    let strtHeader = space.spaceAfter(20, 'STRT  .M');
    let stopHeader = space.spaceAfter(20, 'STOP  .M');
    let stepHeader = space.spaceAfter(20, 'STEP  .M');
    for (i in wellHeaders) {
        if (wellHeaders[i].header === 'STRT' && strtHeader == space.spaceAfter(20, 'STRT  .M')) {
            strtHeader += space.spaceAfter(36, wellHeaders[i].value) + ":" + wellHeaders[i].description;
        }
        if (wellHeaders[i].header === 'TOP' && strtHeader == space.spaceAfter(20, 'STRT  .M')) {
            strtHeader += space.spaceAfter(36, wellHeaders[i].value) + ":" + wellHeaders[i].description;
        }
        if (wellHeaders[i].header === 'STOP') {
            stopHeader += space.spaceAfter(36, wellHeaders[i].value) + ":" + wellHeaders[i].description;
        }
        if (wellHeaders[i].header === 'STEP') {
            stepHeader += space.spaceAfter(36, wellHeaders[i].value) + ":" + wellHeaders[i].description;
        }
    }
    fs.appendFileSync(lasFilePath, strtHeader + '\r\n' + stopHeader + '\r\n' + stepHeader + '\r\n');
    //append other headers
    for (i in wellHeaders) {
        if (wellHeaders[i].value && wellHeaders[i].header !== 'filename' && wellHeaders[i].header !== 'COMPANY' && wellHeaders[i].header !== 'STRT' && wellHeaders[i].header !== 'STOP' && wellHeaders[i].header !== 'STEP') {
            let header = space.spaceAfter(20, wellHeaders[i].header.toString() + '  .') + space.spaceAfter(36, wellHeaders[i].value) + ": " + wellHeaders[i].description + '\r\n';
            fs.appendFileSync(lasFilePath, header);
        }
    }
}

async function writeDataset(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, s3, curveModel, curveBasePath, callback) {
    if (!project && dataset.dataset_params.length > 0) { //export from inventory
        fs.appendFileSync(lasFilePath, '\r\n~' + dataset.name.toUpperCase() + '_PARAMETER\r\n');
        fs.appendFileSync(lasFilePath, '#MNEM.UNIT                    VALUE                        DESCRIPTION\r\n');
        fs.appendFileSync(lasFilePath, '#--------------- ---         ------------                 -----------------\r\n\r\n');
        for (param of dataset.dataset_params) {
            let line = space.spaceAfter(16, param.mnem) + space.spaceAfter(14, '.') + space.spaceAfter(28, param.value) + ': ' + param.description + '\r\n';
            fs.appendFileSync(lasFilePath, line);
        }
    }
    fs.appendFileSync(lasFilePath, '\r\n\r\n~' + dataset.name.toUpperCase() + '_DEFINITION\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT                 LOG CODE                  CURVE DESCRIPTION\r\n');
    fs.appendFileSync(lasFilePath, '#----------- ----         ------------              -----------------\r\n\r\n');
    fs.appendFileSync(lasFilePath, 'DEPTH       .M                                      : Depth    {F}\r\n');

    let top = Number.parseFloat(dataset.top);
    let bottom = Number.parseFloat(dataset.bottom);
    let step = Number.parseFloat(dataset.step);
    let readStreams = [];
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' })

    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) { return curve.idCurve == idCurve });
        let line;
        if (project) { //export from project
            line = space.spaceAfter(12, curve.name) + '.' + space.spaceAfter(39, curve.unit) + ':\r\n';
            let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
            console.log('curvePath', curvePath);
            let stream = byline.createStream(fs.createReadStream(curvePath)).pause();
            readStreams.push(stream);
        } else { //export from inventory
            line = space.spaceAfter(12, curve.name) + '.' + space.spaceAfter(39, curve.curve_revisions[0].unit) + ':\r\n';
            let curvePath = await curveModel.getCurveKey(curve.curve_revisions[0]);
            console.log('curvePath=========', curvePath);
            let stream = byline.createStream(await s3.getData(curvePath)).pause();
            readStreams.push(stream);
        }
        fs.appendFileSync(lasFilePath, line);
    }
    fs.appendFileSync(lasFilePath, '\r\n\r\n' + '~' + dataset.name + '_DATA | ' + dataset.name + '_DEFINITION\r\n');

    //writeCurves
    if (readStreams.length === 0) {
        console.log('hiuhiu', top, bottom, step);
        for (let i = top; i < bottom + step; i += step) {
            writeStream.write(space.spaceBefore(15, i.toFixed(2)) + '\r\n', function () {
                if (i >= bottom) {
                    callback(null, {
                        fileName: fileName,
                        wellName: well.name
                    })
                }
            });
        }
    } else {
        readStreams[0].resume();        
        for (let i = 0; i < readStreams.length; i++) {
            if (i == 0 && readStreams[i].isPaused()) {
                readStreams[i].resume();
            }
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].on('data', function (line) {
                readLine++;
                let tokens = line.toString('utf8').split("||");
                if (tokens == null) {
                    let nullHeader = well.well_headers.find(header => {
                        return header.header == "NULL";
                    })
                    tokens = nullHeader.value;
                }
                tokens = tokens.toString().substring(tokens.toString().indexOf(" ") + 1);
                if (i !== readStreams.length - 1) {
                    tokens += ',';
                }
                tokens = space.spaceBefore(15, tokens);
                if (i === 0) {
                    let depth = top.toFixed(5).toString() + ',';
                    depth = space.spaceBefore(16, depth);
                    tokens = depth + tokens;
                    top += step;
                }
                if (i !== readStreams.length - 1) {
                    writeStream.write(tokens, function () {
                    })
                    readStreams[i].pause();
                    if (readStreams[i + 1].isPaused()) {
                        readStreams[i + 1].resume();
                    }
                } else {
                    writeStream.write(tokens + '\r\n', function () {
                        writeLine++;
                        if (readStreams.numLine && readStreams.numLine === writeLine) {
                            console.log('number of line', readStreams.numLine, writeLine);
                            readLine = 0;
                            writeLine = 0;
                            readStreams.numLine = "";
                            callback(null, {
                                fileName: fileName,
                                wellName: well.name
                            });
                        }
                    });
                    readStreams[i].pause();
                    if (readStreams[0].isPaused()) {
                        readStreams[0].resume();
                    }
                }
            })
            readStreams[i].on('end', function () {
                console.log('end', i, readLine);
                if (!readStreams.numLine) {
                    readStreams.numLine = readLine;
                }
                console.log('END TIME', new Date(), readStreams.numLine);
                if (i != readStreams.length - 1) {
                    console.log('resume', i + 1);
                    readStreams[i + 1].resume()
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


    let fileName = well.name + "_" + Date.now() + '.las'
    fileName = fileName.replace(/\//g, "-");
    lasFilePath = path.join(lasFilePath, fileName);
    writeVersion(lasFilePath);
    writeWellHeader(lasFilePath, well.well_headers);
    if (project) { //export from project
        async.mapSeries(datasetObjs, function (item, cb) {
            console.log('111111', item.idCurves);
            let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == item.idDataset; });
            writeDataset(lasFilePath, exportPath, fileName, project, well, dataset, item.idCurves, null, null, curveBasePath, cb);
        }, function (err, rs) {
            console.log('map series callback');
            if (err) {
                callback(err);
            } else {
                console.log('rs', rs);
                callback(null, rs[0]);
            }
        });
    } else { //export from inventory
        async.mapSeries(datasetObjs, function (item, cb) {
            console.log('callback', cb);
            console.log('111111', item.idCurves);
            let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == item.idDataset; });
            writeDataset(lasFilePath, exportPath, fileName, null, well, dataset, item.idCurves, s3, curveModel, null, cb);
        }, function cb(err, rs) {
            console.log('map series callback');
            if (err) {
                callback(err);
            } else {
                console.log('rs', rs);
                callback(null, rs[0]);
            }
        });
    }
}

module.exports.writeAll = writeAll;
