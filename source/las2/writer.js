let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../space');
let wiImport = require('wi-import');
let hashDir = wiImport.hashDir;
const MDCurve = '__MD';
const WHLEN1 = 19;
const WHLEN2 = 10;
const WHLEN3 = 30;
let _unitTable = null;

module.exports.setUnitTable = setUnitTable;

function setUnitTable(unitTable) {
    _unitTable = unitTable;
}


function convertUnit(value, fromUnit, desUnit) {
    //todo
    if (!_unitTable) return value;
    let unitTable = _unitTable;
    let fromRate = unitTable[fromUnit];
    let desRate = unitTable[desUnit];
    if (fromRate && desRate) {

        return value * desRate / fromRate;
    }
    return value;
}

function writeVersion(lasFilePath) {
    fs.appendFileSync(lasFilePath, '~Version\r\n');
    fs.appendFileSync(lasFilePath, 'VERS .        2      : CWLS LAS Version 2.0 \r\n');
    fs.appendFileSync(lasFilePath, 'WRAP .        NO     : One Line per Depth Step \r\n');
}

function getWellUnit(well) {
    // TODO
    let unitHeader = well.well_headers.find(function (header) {
        return header.header == 'UNIT';
    })
    return unitHeader.value;
}

function writeWellHeader(lasFilePath, well, dataset, from) {
    let wellHeaders = well.well_headers;
    fs.appendFileSync(lasFilePath, '~Well\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, '#\r\n#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    //append start depth, stop depth and step
    let strtHeader;
    let stopHeader;
    let stepHeader;
    let wellUnit = dataset.unit || getWellUnit(well) || 'M';

    strtHeader = space.spaceAfter(WHLEN1, 'STRT') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let topValue = from == 'inventory' ? dataset.top : convertUnit(Number.parseFloat(dataset.top), 'M', wellUnit);
    strtHeader += space.spaceAfter(WHLEN3, Number.parseFloat(topValue).toFixed(4)) + ": Top Depth";

    stopHeader = space.spaceAfter(WHLEN1, 'STOP') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let bottomValue = from == 'inventory' ? dataset.bottom : convertUnit(Number.parseFloat(dataset.bottom), 'M', wellUnit);
    stopHeader += space.spaceAfter(WHLEN3, Number.parseFloat(bottomValue).toFixed(4)) + ": Bottom Depth";

    stepHeader = space.spaceAfter(WHLEN1, 'STEP') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let stepValue = from == 'inventory' ? dataset.step : convertUnit(Number.parseFloat(dataset.step), 'M', wellUnit);
    stepHeader += space.spaceAfter(WHLEN3, Number.parseFloat(stepValue).toFixed(4)) + ": Step";

    totalHeader = space.spaceAfter(WHLEN1, 'TOTAL DEPTH') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let totalValue = Number.parseFloat(bottomValue) - Number.parseFloat(topValue);
    console.log('------', topValue, bottomValue, totalValue);
    totalHeader += space.spaceAfter(WHLEN3, Number.parseFloat(totalValue).toFixed(4)) + ": Total Depth";

    fs.appendFileSync(lasFilePath, strtHeader + '\r\n' + stopHeader + '\r\n' + stepHeader + '\r\n' + totalHeader + '\r\n');
    //append other headers

    let nullHeader = space.spaceAfter(WHLEN1, 'NULL') + space.spaceAfter(WHLEN2, '.') + space.spaceAfter(WHLEN3, '-9999') + ": \r\n";
    fs.appendFileSync(lasFilePath, nullHeader);

    // let WELL_header = wellHeaders.find(function (h) { return h.value == 'WELL' });
    // let NULL_header = wellHeaders.find(function (h) { return h.value == 'NULL' });
    // if (!WELL_header) {
    let wellHeader = space.spaceAfter(WHLEN1, 'WELL') + space.spaceAfter(WHLEN2, '.') + space.spaceAfter(WHLEN3, well.name) + ": " + 'WELL NAME' + '\r\n';
    fs.appendFileSync(lasFilePath, wellHeader);
    // }
    // if (!NULL_header) {
    //     let header = space.spaceAfter(14, " " + 'NULL' + '.') + space.spaceAfter(24, '-999.2500') + ": \r\n";
    //     fs.appendFileSync(lasFilePath, header);
    // }
    for (i in wellHeaders) {
        if (wellHeaders[i].header !== 'filename' && wellHeaders[i].header !== 'STRT' && wellHeaders[i].header !== 'STOP' && wellHeaders[i].header !== 'STEP' && wellHeaders[i].header !== 'NULL' && wellHeaders[i].header !== 'WELL' && wellHeaders[i].header != 'TOTAL DEPTH') {
            let header = space.spaceAfter(WHLEN1, wellHeaders[i].header.toString()) + space.spaceAfter(WHLEN2, '.' + wellHeaders[i].unit) + space.spaceAfter(WHLEN3, wellHeaders[i].value) + ": " + wellHeaders[i].description + '\r\n';
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

    fs.appendFileSync(lasFilePath, '~Parameter\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, '#\r\n#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');

    fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, "SET") + space.spaceAfter(10, '.') + space.spaceAfter(WHLEN3, dataset.name) + ': \r\n');
    if (dataset.dataset_params) {
        for (param of dataset.dataset_params) {
            fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, param.mnem) + space.spaceAfter(WHLEN2, '.' + param.unit) + space.spaceAfter(WHLEN3, param.value) + ": " + param.description + '\r\n');
        }
    }

    fs.appendFileSync(lasFilePath, '~Curve\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#LOGNAME           .UNIT     LOG_ID                        : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, 'DEPTH') + space.spaceAfter(WHLEN2 + WHLEN3, '.' + dataset.unit) + ':\r\n');


    if (!well) {  //export from project
        well = project.wells[0];
        well.username = project.createdBy;
    }

    let fromUnit = dataset.unit || 'M';
    if(project) fromUnit = 'M';

    let top = convertUnit(Number.parseFloat(dataset.top), fromUnit, dataset.unit);
    let bottom = convertUnit(Number.parseFloat(dataset.bottom), fromUnit, dataset.unit);
    let step = convertUnit(Number.parseFloat(dataset.step), fromUnit, dataset.unit);
    let readStreams = [];
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' });
    let curveColumns = '~A        DEPTH';

    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) {
            return curve.idCurve == idCurve
        });
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
                fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, curve.name) + space.spaceAfter(WHLEN2 + WHLEN3, '.' + curve.curve_revisions[0].unit) + ': ' + curve.description + '\r\n');
            } else { //export from project
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                console.log('curvePath', curvePath);
                stream = fs.createReadStream(curvePath);
                fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, curve.name) + space.spaceAfter(WHLEN2 + WHLEN3, '.' + curve.unit) + ': ' + curve.description + '\r\n');
            }
            stream = byline.createStream(stream).pause();
            readStreams.push(stream);

            if (idCurve == 0)
                curveColumns += space.spaceBefore(15, curve.name);
            else
                curveColumns += space.spaceBefore(18, curve.name);

        }
    }

    fs.appendFileSync(lasFilePath, curveColumns + '\r\n');
    // fs.appendFileSync(lasFilePath, curveColumns + '\r\n');

    //write curves
    if (readStreams.length === 0 || idCurves.length == 0) {
        if (step == 0) {
            callback(null, {
                fileName: fileName,
                wellName: well.name,
                datasetName: dataset.name
            })
        } else
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
        for (let i = 0; i < readStreams.length; i++) {
            console.log('~', readStreams[i].isPaused());
        }
        for (let i = 0; i < readStreams.length; i++) {
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].on('data', function (line) {
                readLine++;
                let tokens = line.toString('utf8').split("||");
                let index = tokens.toString().substring(0, tokens.toString().indexOf(" "));
                tokens = tokens.toString().substring(tokens.toString().indexOf(" ") + 1);
                let _ = require('lodash');
                if (!_.isFinite(parseFloat(tokens))) {
                    // let nullHeader = well.well_headers.find(header => {
                    //     return header.header == "NULL";
                    // })
                    // tokens = nullHeader ? nullHeader.value :  '-999.0000';
                    tokens = '-9999';
                } else tokens = parseFloat(tokens).toFixed(4);
                tokens = space.spaceBefore(18, tokens);
                if (i === 0) {
                    index = Number(index);
                    let depth, hasDepth;
                    if (step == 0 || hasDepth) {
                        depth = convertUnit(index, 'M', dataset.unit).toFixed(4);
                        hasDepth = true;
                    } else {
                        depth = top.toFixed(4);
                        top += step;
                    }

                    depth = space.spaceBefore(15, depth);
                    tokens = depth + tokens;
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
                if (!readStreams.numLine) {
                    readStreams.numLine = readLine;
                    console.log('numLine', readStreams.numLine);
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

    let dataset = well.datasets.find(function (dataset) {
        return dataset.idDataset == idDataset;
    });
    if (dataset) {
        let fileName = dataset.name + "_" + well.name + "_" + Date.now() + '.las'
        fileName = fileName.replace(/\//g, "-");
        lasFilePath = path.join(lasFilePath, fileName);
        let from = project ? 'project' : 'inventory';
        writeVersion(lasFilePath);
        writeWellHeader(lasFilePath, well, dataset, from);
        writeCurve(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, s3, curveModel, curveBasePath, function (err, rs) {
            console.log('writeAll callback called', rs);
            if (err) {
                callback(err);
            } else {
                callback(null, rs);
            }
        });
        // }
    } else {
        console.log('no dataset');
        callback(null, null);
    }
}

module.exports.writeAll = writeAll;
