let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../space');
let wiImport = require('wi-import');
let hashDir = wiImport.hashDir;

function writeVersion(lasFilePath) {
    fs.appendFileSync(lasFilePath, '~Version\r\n');
    fs.appendFileSync(lasFilePath, 'VERS .        2      : CWLS LAS Version 2.0 \r\n');
    fs.appendFileSync(lasFilePath, 'WRAP .        NO     : One Line per Depth Step \r\n');
}

function writeWellHeader(lasFilePath, wellHeaders) {
    fs.appendFileSync(lasFilePath, '~Well\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT       Data Type                    Information\r\n');
    fs.appendFileSync(lasFilePath, '#---------       ---------                    -------------------------------\r\n');
    //append start depth, stop depth and step
    let strtHeader = ' STRT.M       ';
    let stopHeader = ' STOP.M       ';
    let stepHeader = ' STEP.M       ';
    for (i in wellHeaders) {
        if (wellHeaders[i].header === 'STRT' && strtHeader == ' STRT.M       ') {
            strtHeader += space.spaceAfter(36, wellHeaders[i].value) + ":" + wellHeaders[i].description;
        }
        if(wellHeaders[i].header === 'TOP' && strtHeader == ' STRT.M       ') {
            strtHeader += space.spaceAfter(36, wellHeaders[i].value) + ":" + wellHeaders[i].description;            
        }
        if (wellHeaders[i].header === 'STOP') {
            stopHeader += space.spaceAfter(24, wellHeaders[i].value) + ": " + wellHeaders[i].description;
        }
        if (wellHeaders[i].header === 'STEP') {
            stepHeader += space.spaceAfter(24, wellHeaders[i].value) + ": " + wellHeaders[i].description;
        }
    }
    fs.appendFileSync(lasFilePath, strtHeader + '\r\n' + stopHeader + '\r\n' + stepHeader + '\r\n');
    //append other headers
    for (i in wellHeaders) {
        if (wellHeaders[i].value && wellHeaders[i].header !== 'filename' && wellHeaders[i].header !== 'STRT' && wellHeaders[i].header !== 'STOP' && wellHeaders[i].header !== 'STEP') {
            let header = space.spaceAfter(14, " " + wellHeaders[i].header.toString() + '.') + space.spaceAfter(24, wellHeaders[i].value) + ": " + wellHeaders[i].description + '\r\n';
            fs.appendFileSync(lasFilePath, header);
        }
    }
}

async function writeCurve(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, s3, curveModel, curveBasePath, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, s3, curveModel are null
    */
    fs.appendFileSync(lasFilePath, '~Curve\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT       API Code            Curve    Description\r\n');
    fs.appendFileSync(lasFilePath, '#--------        --------------      -----    -------------------\r\n');
    fs.appendFileSync(lasFilePath, 'DEPTH.M  :\r\n');

    if (!well) {  //export from project
        well = project.wells[0];
        well.username = project.createdBy;
    }

    let top = Number.parseFloat(dataset.top);
    let bottom = Number.parseFloat(dataset.bottom);
    let step = Number.parseFloat(dataset.step);
    let readStreams = [];
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' });
    let curveColumns = '~A        DEPTH';

    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) { return curve.idCurve == idCurve });
        if (curve) {
            if (!project) { //export from inventory
                let curvePath = await curveModel.getCurveKey(curve.curve_revisions[0]);
                console.log('curvePath=========', curvePath);
                let stream = await s3.getData(curvePath);
                stream = byline.createStream(stream).pause();
                readStreams.push(stream);
                fs.appendFileSync(lasFilePath, space.spaceAfter(46, curve.name + '.' + curve.curve_revisions[0].unit + '  :') + curve.description + '\r\n');
            } else { //export from project
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                console.log('curvePath', curvePath);
                let stream = fs.createReadStream(curvePath);
                stream = byline.createStream(stream).pause();
                readStreams.push(stream);
                fs.appendFileSync(lasFilePath, curve.name + '.' + curve.unit + '  :\r\n');
            }
            if (idCurve == 0) {
                curveColumns += space.spaceBefore(15, curve.name);
            } else {
                curveColumns += space.spaceBefore(18, curve.name);
            }

        }
    }

    fs.appendFileSync(lasFilePath, '~Parameter\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT       Value                        Description\r\n');
    fs.appendFileSync(lasFilePath, '#---------       ---------                    -------------\r\n');

    if (dataset.dataset_params) {
        for (param of dataset.dataset_params) {
            fs.appendFileSync(lasFilePath, space.spaceAfter(17, param.mnem) + space.spaceAfter(29, param.value) + param.description + '\r\n');
        }
    }
    fs.appendFileSync(lasFilePath, curveColumns + '\r\n');

    //write curves
    if (readStreams.length === 0) {
        for (let i = top; i < bottom + step; i += step) {
            writeStream.write(space.spaceBefore(15, i.toFixed(2)) + '\r\n', function () {
                if (i >= bottom) {
                    callback(null, {
                        fileName: fileName,
                        wellName: well.name,
                        datasetName: dataset.name
                    })
                }
            });
        }
    } else {
        readStreams[0].resume();
        for(let i = 0; i< readStreams.length; i++) {
            console.log('~', readStreams[i].isPaused());
        }
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
                tokens = space.spaceBefore(18, tokens);
                if (i === 0) {
                    let depth = top.toFixed(4).toString();
                    depth = space.spaceBefore(15, depth);
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
                            callback(null, {
                                fileName: fileName,
                                wellName: well.name,
                                datasetName: dataset.name
                            });
                        }
                    });
                    readStreams[i].pause();
                    if (readStreams[0].isPaused()) {
                        readStreams[0].resume();
                    }
                }
            })
            readStreams[i].on('error', function (err) {
                console.log('-------', error);
                callback(err);
            })
            readStreams[i].on('end', function () {
                if(!readStreams.numLine) {
                    readStreams.numLine = readLine;   
                    console.log('numLine', readStreams.numLine);                 
                }
                if(readLine == 0) {
                    callback('No curve data');
                }
                console.log('END TIME', new Date(), readStreams.numLine);
                if (i != readStreams.length - 1) {
                    console.log('---', i, readStreams.length-1);
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

    let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == idDataset; });
    if (dataset) {
        let fileName = dataset.name + "_" + well.name + "_" + Date.now() + '.las'
        fileName = fileName.replace(/\//g, "-");
        lasFilePath = path.join(lasFilePath, fileName);
        writeVersion(lasFilePath);
        writeWellHeader(lasFilePath, well.well_headers);
        if (project) { //export from project
            writeCurve(lasFilePath, exportPath, fileName, project, null, dataset, idCurves, null, null, curveBasePath, function(err, rs) {
                console.log('writeAll callback called', rs);
                if(err) {
                    callback(err);
                } else {
                    callback(null, rs);
                }
            });
        } else { //export from inventory
            writeCurve(lasFilePath, exportPath, fileName, null, well, dataset, idCurves, s3, curveModel, null, function(err, rs) {
                console.log('writeAll callback called', rs);
                if(err) {
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
