let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../space');
let wiImport = require('wi-import');
let hashDir = wiImport.hashDir;
const MDCurve = '__MD';
let _unitTable = null;
let _wellTopDepth;
let _wellBottomDepth;
let _wellStep;
let _wellUnit;

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
    fs.appendFileSync(lasFilePath, 'VERS  .             3.00                                : CWLS LOG ASCII STANDARD - VERSION 3.00\r\n');
    fs.appendFileSync(lasFilePath, 'WRAP  .             NO                                  : One line per depth step\r\n');
    fs.appendFileSync(lasFilePath, 'DLM   .             COMMA                               : Delimiter character between data columns\r\n');
    fs.appendFileSync(lasFilePath, '#Delimiting character ( SPACE TAB OR COMMA ).\r\n\r\n');
}

function writeWellHeader(lasFilePath, well) {
    let wellHeaders = well.well_headers;
    fs.appendFileSync(lasFilePath, '~Well\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT          DATA                                DESCRIPTION\r\n');
    fs.appendFileSync(lasFilePath, '#----- ----        -----------------                   -----------\r\n');
    //append start depth, stop depth and step
    let wellUnit = _wellUnit || well.unit || 'M';
    let strtHeader = space.spaceAfter(20, 'STRT  .' + wellUnit);
    let stopHeader = space.spaceAfter(20, 'STOP  .' + wellUnit);
    let stepHeader = space.spaceAfter(20, 'STEP  .' + wellUnit);
    let totalHeader = space.spaceAfter(20, 'TOTAL DEPTH  .' + wellUnit);
    strtHeader += space.spaceAfter(36, Number.parseFloat(_wellTopDepth).toFixed(4)) + ": Top Depth";
    stopHeader += space.spaceAfter(36, Number.parseFloat(_wellBottomDepth).toFixed(4)) + ": Bottom Depth";
    stepHeader += space.spaceAfter(36, Number.parseFloat(_wellStep).toFixed(4)) + ": Step";
    totalHeader += space.spaceAfter(36,
        (Number.parseFloat(_wellBottomDepth) -
            Number.parseFloat(_wellTopDepth)).toFixed(4)) + ": Total Depth";

    fs.appendFileSync(lasFilePath, strtHeader + '\r\n' + stopHeader + '\r\n' + stepHeader + '\r\n' + totalHeader + '\r\n');
    //append other headers
    let nullHeader = space.spaceAfter(20, 'NULL' + '.') + space.spaceAfter(36, '-9999') + ": NULL VALUE\r\n";
    fs.appendFileSync(lasFilePath, nullHeader);

    let wellHeader = space.spaceAfter(20, 'WELL' + '.') + space.spaceAfter(36, well.name) + ": " + 'WELL NAME' + '\r\n';
    fs.appendFileSync(lasFilePath, wellHeader);

    for (i in wellHeaders) {
        if (wellHeaders[i].value && wellHeaders[i].header !== 'filename' && wellHeaders[i].header !== 'COMPANY' && wellHeaders[i].header !== 'STRT' && wellHeaders[i].header !== 'STOP' && wellHeaders[i].header !== 'STEP' && wellHeaders[i].header != 'NULL' && wellHeaders[i].header != 'WELL' && wellHeaders[i].header != 'TOTAL DEPTH') {
            let header = space.spaceAfter(20, wellHeaders[i].header.toString() + '  .' + wellHeaders[i].unit) + space.spaceAfter(36, wellHeaders[i].value) + ": " + wellHeaders[i].description + '\r\n';
            fs.appendFileSync(lasFilePath, header);
        }
    }
}

async function writeDataset(lasFilePath, fileName, project, well, dataset, idCurves, s3, curveModel, curveBasePath, callback) {
    fs.appendFileSync(lasFilePath, '\r\n~' + dataset.name.toUpperCase().replace(/ /g, ".").replace(/_DATA/, "") + '_PARAMETER\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT                    VALUE                        DESCRIPTION\r\n');
    fs.appendFileSync(lasFilePath, '#--------------- ---         ------------                 -----------------\r\n\r\n');
    fs.appendFileSync(lasFilePath, space.spaceAfter(16, 'SET') + space.spaceAfter(14, '.') + space.spaceAfter(28, dataset.name) + ':\r\n');
    if (dataset.dataset_params && dataset.dataset_params.length > 0) {
        for (param of dataset.dataset_params) {
            let line = space.spaceAfter(16, param.mnem) + space.spaceAfter(14, '.' + param.unit) + space.spaceAfter(28, param.value) + ': ' + param.description + '\r\n';
            fs.appendFileSync(lasFilePath, line);
        }
    }
    fs.appendFileSync(lasFilePath, '\r\n\r\n~' + dataset.name.toUpperCase().replace(/ /g, ".").replace(/_DATA/, "") + '_DEFINITION\r\n');
    fs.appendFileSync(lasFilePath, '#MNEM.UNIT                 LOG CODE                  CURVE DESCRIPTION\r\n');
    fs.appendFileSync(lasFilePath, '#----------- ----         ------------              -----------------\r\n\r\n');
    fs.appendFileSync(lasFilePath, 'DEPTH       .M                                      : Depth    {F}\r\n');

    let desUnit = dataset.unit || 'M';
    if (!project) {
        desUnit = 'M';
    }
    let top = convertUnit(Number.parseFloat(dataset.top), 'M', desUnit);
    let bottom = convertUnit(Number.parseFloat(dataset.bottom), 'M', desUnit);
    let step = convertUnit(Number.parseFloat(dataset.step), 'M', desUnit);
    let readStreams = [];
    let writeStream = fs.createWriteStream(lasFilePath, {flags: 'a'})

    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) {
            return curve.idCurve == idCurve
        });
        let line;
        if (curve && curve.name != MDCurve) {
            let stream;
            if (project) { //export from project
                line = space.spaceAfter(12, curve.name) + '.' + space.spaceAfter(39, curve.unit) + ': ' + curve.description + '\r\n';
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                console.log('curvePath', curvePath);
                stream = fs.createReadStream(curvePath);
            } else { //export from inventory
                line = space.spaceAfter(12, curve.name) + '.' + space.spaceAfter(39, curve.curve_revisions[0].unit) + ': ' + curve.description + '\r\n';
                let curvePath = await curveModel.getCurveKey(curve.curve_revisions[0]);
                console.log('curvePath=========', curvePath);
                try {
                    stream = await s3.getData(curvePath)
                } catch (e) {
                    console.log('=============NOT FOUND CURVE FROM S3', e);
                    callback(e);
                }
            }
            stream = byline.createStream(stream).pause();
            readStreams.push(stream);
            fs.appendFileSync(lasFilePath, line);
        }
    }
    fs.appendFileSync(lasFilePath, '\r\n\r\n' + '~' + dataset.name.replace(/ /g, ".").replace(/_DATA/, "") + '_DATA | ' + dataset.name.replace(/ /g, ".") + '_DEFINITION\r\n');

    //writeCurves
    if (readStreams.length === 0 || idCurves.length == 0) {
        console.log('hiuhiu', top, bottom, step);
        if(step == 0) {
            callback(null, {
                fileName: fileName,
                wellName: well.name
            })
        } else 
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
        let hasDepth;
        for (let i = 0; i < readStreams.length; i++) {
            if (i == 0 && readStreams[i].isPaused()) {
                readStreams[i].resume();
            }
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].on('data', function (line) {
                readLine++;
                let tokens = line.toString('utf8').split("||");
                let index = tokens.toString().substring(0, tokens.toString().indexOf(" "));
                tokens = tokens.toString().substring(tokens.toString().indexOf(" ") + 1);
                // if (tokens == null || tokens == NaN || tokens.substring(0, 4) == 'null' || tokens == 'NaN' || !tokens) {
                let _ = require('lodash');
                if (!_.isFinite(parseFloat(tokens))) {
                    // let nullHeader = well.well_headers.find(header => {
                    //     return header.header == "NULL";
                    // })
                    // tokens = nullHeader ? nullHeader.value :  '-999.0000';
                    tokens = '-9999';
                } else tokens = parseFloat(tokens).toFixed(4);
                tokens = space.spaceBefore(15, tokens);
                if (i === 0) {
                    index = Number(index);
                    let depth;
                    if (step == 0 || hasDepth) {
                        depth = index.toFixed(4).toString() + ',';
                        hasDepth = true;
                    } else {
                        depth = top.toFixed(4).toString() + ',';
                        top += step;
                    }
                    depth = space.spaceBefore(16, depth);
                    tokens = depth + tokens;
                }
                if (i !== readStreams.length - 1) {
                    tokens += ',';
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
                if (i == readStreams.length - 1 && readLine == 0) {
                    callback('No curve data');
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

    if(datasetObjs.length > 1) {
        _wellBottomDepth = convertUnit(getWellBottomDepth(well), 'M', well.unit);
        _wellTopDepth = convertUnit(getWellTopDepth(well), 'M', well.unit);
        _wellStep = convertUnit(getWellStep(well), 'M', well.unit);
        _wellUnit = well.unit;
    } else {
        let dataset = well.datasets.find(function (dataset) {
            return dataset.idDataset == datasetObjs[0].idDataset;
        });
        _wellBottomDepth = dataset.bottom;
        _wellTopDepth = dataset.top;
        _wellStep = dataset.step;
        _wellUnit = dataset.unit;
    }

    let fileName = well.name + "_" + Date.now() + '.las'
    fileName = fileName.replace(/\//g, "-");
    lasFilePath = path.join(lasFilePath, fileName);
    writeVersion(lasFilePath);
    writeWellHeader(lasFilePath, well);
    async.mapSeries(datasetObjs, function (item, cb) {
        console.log('callback', cb);
        console.log('111111', item.idCurves);
        let dataset = well.datasets.find(function (dataset) {
            return dataset.idDataset == item.idDataset;
        });
        writeDataset(lasFilePath, fileName, project, well, dataset, item.idCurves, s3, curveModel, curveBasePath, cb);
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

function getWellTopDepth(well) {
    let datasets = well.datasets;
    return Math.min(...datasets.map(d => +convertUnit(d.top, d.unit, 'M')));
}

function getWellBottomDepth(well) {
    let datasets = well.datasets;
    return Math.max(...datasets.map(d => +convertUnit(d.bottom, d.unit, 'M')));
}

function getWellStep(well) {
    let datasets = well.datasets;
    return datasets[0] ? convertUnit(datasets[0].step, dataset[0].unit, 'M') : 0;
}

module.exports.writeAll = writeAll;
