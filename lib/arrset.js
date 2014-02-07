// Really simple array-as-set math functions.
var equal = require('deep-equal');

// Returns if a member is not in a given array (for unique filtering).
function notIn(arr) {
  return function (el) {
    for (var i = 0; i < arr.length; i++){
      if (equal(el,arr[i])) return false;
    }
    return true;
  };
}
exports.notIn = notIn;

function equalSets(arr1, arr2) {
  return arr1.filter(notIn(arr2)).length == 0
      && arr2.filter(notIn(arr1)).length == 0;
}
exports.equal = equalSets;
