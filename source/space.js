module.exports.spaceBefore = function (width, string) {
    let l = string.length;
    for (let i = 0; i < width - l; i++) {
        string = " " + string;
    }
    return string;
}
module.exports.spaceAfter = function (width, string) {
    let l = string.length;
    for (let i = 0; i < width - l; i++) {
        string = string + " ";
    }
    return string;
}